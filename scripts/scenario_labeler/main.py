from scripts.scenario_labeler.model.rule_loader import load_rules
from scripts.scenario_labeler.core.fds_engine import FDSEngine

if __name__ == "__main__":
    rules = load_rules("resources/rules.yaml")
    engine = FDSEngine(rules)

    sample_data = [
        {"@timestamp": "2025-05-01 13:00:00", "@id":"f3ab9ff6785148ca8b419bb9269bb672", "amount": 12000000, "account_no": "1111", "channel_type": "ATM"},
        {"@timestamp": "2025-05-01 14:00:00", "@id":"44d29e4ab6ca48369380da7cc8cb60d1", "amount": 300000, "account_no": "1111", "channel_type": "APP"}
    ]

    detected = engine.evaluate(sample_data)
    print(f"incident: {detected}")
