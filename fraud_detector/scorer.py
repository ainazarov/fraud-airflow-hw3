"""Shared training/inference features, versioned reload before each transaction."""
import os
import pandas as pd
from pipeline.registry import HotReloadingModel


class FraudScorer:
    def __init__(self):
        self.model = HotReloadingModel(os.environ.get("STORAGE_DIR", "/opt/project/storage"))
        self.threshold = float(os.environ.get("FRAUD_THRESHOLD", "0.5"))

    def score(self, data):
        scores, version = self.model.predict(pd.DataFrame([data]))
        score = float(scores[0])
        return {"score": score, "fraud_flag": int(score > self.threshold), "model_version": version}
