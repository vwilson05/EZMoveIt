import os

def load_slack_config():
    """
    Directly sets the Slack webhook URL for local development.
    In production, you might set this via secrets or environment variables.
    """
    # Replace the URL below with your actual Slack webhook URL.
    os.environ["SLACK_WEBHOOK_URL"] = "https://hooks.slack.com/services/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"
    print("Slack webhook URL set.")

if __name__ == "__main__":
    load_slack_config()
