from maritimeiq.eval.harness import run_eval
import json

if __name__ == "__main__":
    print(json.dumps(run_eval(), indent=2))
