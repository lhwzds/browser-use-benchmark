# RestFlow BU Bench Result

This repository includes a local adapter for running `BU Bench V1` with RestFlow as the execution backend while keeping the benchmark task set and Gemini judge unchanged.

## Result

- Framework: `RestFlow`
- Model: `gpt-5.4`
- Judge: `gemini-2.5-flash`
- Benchmark: `BU Bench V1`
- Tasks completed: `100`
- Tasks successful: `78`
- Success rate: `78.0%`
- Run start: `20260417_225149`
- Total steps: `99`
- Total duration: `13496.515814079496` seconds
- Total cost recorded by harness: `$0.00`

## Notes

- This score was produced with the RestFlow IPC adapter in this fork, not the stock `browser_use.Agent(...)` runner.
- The benchmark task set and judge logic were preserved from upstream.
- Upstream benchmark runtime stores raw traces under ignored directories such as `run_data/` and `results/`; this tracked summary exists so the result can be versioned in Git.
