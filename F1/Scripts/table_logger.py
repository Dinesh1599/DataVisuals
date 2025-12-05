import logging
import os

class TableLogger:
    def __init__(self, table_name, log_dir="./logs"):
        self.table_name = table_name

        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"{table_name}.log")

        self.logger = logging.getLogger(table_name)
        self.logger.setLevel(logging.INFO)

        # Prevent duplicate handlers
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter=logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def info(self, msg):
        self.logger.info(msg)

    def warn(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)
