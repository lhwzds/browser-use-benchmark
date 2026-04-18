from __future__ import annotations

import asyncio
import json
import os
import struct
import time
import uuid
from pathlib import Path


MAX_IPC_MESSAGE_SIZE = 16 * 1024 * 1024


def default_restflow_socket_path() -> Path:
    base_dir = os.environ.get("RESTFLOW_DIR")
    if base_dir:
        return Path(base_dir).expanduser() / "restflow.sock"
    return Path.home() / ".restflow" / "restflow.sock"


def _truncate(text: str, limit: int = 2000) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 20] + "...[truncated]..."


def _safe_json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _extract_json(value: str):
    try:
        return json.loads(value)
    except Exception:
        return None


def _collect_screenshot_paths(value, out: list[str]) -> None:
    if isinstance(value, dict):
        if value.get("type") == "screenshot" and isinstance(value.get("path"), str):
            out.append(value["path"])
        for child in value.values():
            _collect_screenshot_paths(child, out)
        return

    if isinstance(value, list):
        for child in value:
            _collect_screenshot_paths(child, out)


class RestFlowIpcError(RuntimeError):
    pass


class RestFlowIpcClient:
    def __init__(self, socket_path: str | Path | None = None):
        self.socket_path = Path(socket_path or default_restflow_socket_path())

    async def _open(self):
        if not self.socket_path.exists():
            raise RestFlowIpcError(
                f"RestFlow socket not found: {self.socket_path}. Start the daemon first."
            )
        return await asyncio.open_unix_connection(str(self.socket_path))

    async def _write_frame(self, writer: asyncio.StreamWriter, payload: dict) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        writer.write(struct.pack("<I", len(encoded)))
        writer.write(encoded)
        await writer.drain()

    async def _read_frame(self, reader: asyncio.StreamReader) -> dict:
        header = await reader.readexactly(4)
        size = struct.unpack("<I", header)[0]
        if size > MAX_IPC_MESSAGE_SIZE:
            raise RestFlowIpcError(f"IPC frame too large: {size}")
        body = await reader.readexactly(size)
        return json.loads(body.decode("utf-8"))

    async def request(self, request_type: str, data: dict | None = None):
        reader, writer = await self._open()
        try:
            payload = {"type": request_type}
            if data is not None:
                payload["data"] = data
            await self._write_frame(writer, payload)
            response = await self._read_frame(reader)
        finally:
            writer.close()
            await writer.wait_closed()

        response_type = str(response.get("response_type", "")).lower()
        if response_type == "success":
            return response.get("data")
        if response_type == "error":
            error = response.get("data") or {}
            raise RestFlowIpcError(error.get("message", "Unknown IPC error"))
        if response_type == "pong":
            return None
        raise RestFlowIpcError(f"Unexpected IPC response: {response}")

    async def stream(self, request_type: str, data: dict, idle_timeout_secs: float = 10.0):
        reader, writer = await self._open()
        frames = []
        try:
            await self._write_frame(writer, {"type": request_type, "data": data})
            while True:
                try:
                    frame = await asyncio.wait_for(
                        self._read_frame(reader), timeout=idle_timeout_secs
                    )
                except asyncio.TimeoutError:
                    if frames:
                        break
                    raise RestFlowIpcError(
                        f"Timed out waiting for IPC stream frame after {idle_timeout_secs}s"
                    )
                frames.append(frame)
                stream_type = frame.get("stream_type")
                if stream_type in {"done", "error"}:
                    break
        finally:
            writer.close()
            await writer.wait_closed()
        return frames


class RestFlowBenchmarkRunner:
    def __init__(
        self,
        model: str,
        socket_path: str | Path | None = None,
        agent_id: str | None = None,
    ):
        self.model = model
        self.agent_id = agent_id
        self.client = RestFlowIpcClient(socket_path)

    async def _resolve_agent_id(self) -> str:
        if self.agent_id:
            agents = await self.client.request("ListAgents")
            for agent in agents:
                if agent.get("id") == self.agent_id or agent.get("name") == self.agent_id:
                    return agent["id"]
            for agent in agents:
                if isinstance(agent.get("id"), str) and agent["id"].startswith(self.agent_id):
                    return agent["id"]
            raise RestFlowIpcError(f"RestFlow agent not found: {self.agent_id}")

        agents = await self.client.request("ListAgents")
        if not agents:
            raise RestFlowIpcError("No RestFlow agents found")

        for agent in agents:
            name = str(agent.get("name", "")).strip().lower()
            if name in {"default", "default assistant"}:
                return agent["id"]

        if len(agents) == 1:
            return agents[0]["id"]

        raise RestFlowIpcError(
            "Multiple RestFlow agents exist. Pass --restflow-agent-id explicitly."
        )

    def _frames_to_steps_and_screenshots(self, frames: list[dict]) -> tuple[list[str], list[str]]:
        steps: list[str] = []
        screenshot_paths: list[str] = []
        data_buffer: list[str] = []

        def flush_data_buffer():
            if not data_buffer:
                return
            content = "".join(data_buffer).strip()
            data_buffer.clear()
            if content:
                steps.append(f"assistant_output: {_truncate(content)}")

        for frame in frames:
            stream_type = frame.get("stream_type")
            data = frame.get("data")

            if stream_type == "ack":
                content = (data or {}).get("content", "")
                if content.strip():
                    steps.append(f"assistant_ack: {_truncate(content.strip())}")
                continue

            if stream_type == "data":
                content = (data or {}).get("content", "")
                if content:
                    data_buffer.append(content)
                continue

            if stream_type == "tool_call":
                flush_data_buffer()
                tool_name = (data or {}).get("name", "unknown")
                arguments = (data or {}).get("arguments", {})
                steps.append(
                    f"tool_call[{tool_name}]: {_truncate(_safe_json_dumps(arguments), 3000)}"
                )
                continue

            if stream_type == "tool_result":
                flush_data_buffer()
                result = (data or {}).get("result", "")
                success = bool((data or {}).get("success", False))
                parsed = _extract_json(result)
                if parsed is not None:
                    _collect_screenshot_paths(parsed, screenshot_paths)
                    rendered = _safe_json_dumps(parsed)
                else:
                    rendered = result
                steps.append(
                    f"tool_result[success={str(success).lower()}]: {_truncate(rendered, 3000)}"
                )
                continue

            if stream_type == "error":
                flush_data_buffer()
                message = (data or {}).get("message", "Unknown stream error")
                steps.append(f"stream_error: {_truncate(message)}")
                continue

        flush_data_buffer()
        return steps, screenshot_paths

    @staticmethod
    def _final_result_from_session(session: dict) -> str:
        messages = session.get("messages") or []
        for message in reversed(messages):
            if message.get("role") == "assistant":
                content = str(message.get("content", "")).strip()
                if content:
                    return content
        return "Agent did not return a result"

    @staticmethod
    def _steps_from_session_messages(session: dict) -> list[str]:
        steps: list[str] = []
        for message in session.get("messages") or []:
            role = str(message.get("role", "")).strip().lower()
            content = str(message.get("content", "")).strip()
            if not content:
                continue
            if role == "assistant":
                steps.append(f"assistant: {_truncate(content, 3000)}")
            elif role == "user":
                steps.append(f"user: {_truncate(content, 1000)}")
        return steps

    async def run_task(self, task_text: str) -> dict:
        agent_id = await self._resolve_agent_id()
        session = await self.client.request(
            "CreateSession",
            {
                "agent_id": agent_id,
                "model": self.model,
                "name": f"benchmark-{uuid.uuid4()}",
                "skill_id": None,
            },
        )
        session_id = session["id"]

        started_at = time.perf_counter()
        try:
            session = await self.client.request(
                "ExecuteChatSession",
                {
                    "session_id": session_id,
                    "user_input": task_text,
                },
            )
            duration = time.perf_counter() - started_at
            frames: list[dict] = []
        finally:
            try:
                await self.client.request("DeleteSession", {"id": session_id})
            except Exception:
                pass

        final_result = self._final_result_from_session(session)
        agent_steps = self._steps_from_session_messages(session)
        screenshot_paths: list[str] = []
        total_tokens = 0
        for frame in frames:
            if frame.get("stream_type") == "done":
                total_tokens = ((frame.get("data") or {}).get("total_tokens")) or 0
                break

        steps = max(0, len(agent_steps) - 1)
        return {
            "final_result": final_result,
            "agent_steps": agent_steps,
            "screenshot_paths": screenshot_paths,
            "steps": steps,
            "duration": duration,
            "cost": 0,
            "total_tokens": total_tokens,
            "session_id": session_id,
            "agent_id": agent_id,
        }
