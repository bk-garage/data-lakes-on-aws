import math
import uuid

from .base_interface import BaseInterface


class SQSInterface(BaseInterface):
    def __init__(self, team=None, dataset=None, pipeline=None, stage=None, log_level=None, session=None):
        super().__init__(team, dataset, pipeline, stage, log_level, session)

    def _initialize_client(self):
        """Initialize SQS client"""
        self.sqs = self.session.client("sqs", config=self.session_config)

    def _load_config(self):
        """Load SQS-specific configuration from SSM"""
        if self.team and self.stage and self.pipeline:
            self.stage_queue = self._get_ssm_parameter(f"/SDLF/SQS/{self.team}/{self.pipeline}{self.stage}Queue")
            self.stage_dlq = self._get_ssm_parameter(f"/SDLF/SQS/{self.team}/{self.pipeline}{self.stage}DLQ")

    @property
    def stage_queue_url(self):
        """Get stage queue URL"""
        return self.sqs.get_queue_url(QueueName=self.stage_queue)["QueueUrl"]

    @property
    def stage_dlq_url(self):
        """Get stage DLQ URL"""
        return self.sqs.get_queue_url(QueueName=self.stage_dlq)["QueueUrl"]

    def receive_messages(self, max_num_messages=1, queue_url=None):
        queue_url = queue_url or self.stage_queue_url
        messages = self.sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=max_num_messages, WaitTimeSeconds=1
        ).get("Messages", [])
        for message in messages:
            self.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
        return messages

    def receive_min_max_messages(self, min_items_process=1, max_items_process=100, queue_url=None):
        """Gets max_items_process messages from an SQS queue.
        :param min_items_process: Minimum number of items to process.
        :param max_items_process: Maximum number of items to process.
        :return messages obtained
        """
        messages = []
        queue_url = queue_url or self.stage_queue_url
        num_messages_queue = int(
            self.sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"])[
                "Attributes"
            ]["ApproximateNumberOfMessages"]
        )

        # If not enough items to process, break with no messages
        if (num_messages_queue == 0) or (min_items_process > num_messages_queue):
            self.logger.info("Not enough messages - exiting")
            return messages

        # Only pull batch sizes of max_batch_size
        num_messages_queue = min(num_messages_queue, max_items_process)
        max_batch_size = 10
        batch_sizes = [max_batch_size] * math.floor(num_messages_queue / max_batch_size)
        if num_messages_queue % max_batch_size > 0:
            batch_sizes += [num_messages_queue % max_batch_size]

        for batch_size in batch_sizes:
            resp_msg = self.receive_messages(max_num_messages=batch_size)
            try:
                messages.extend(message["Body"] for message in resp_msg)
            except KeyError:
                break
        return messages

    def send_message_to_fifo_queue(self, message, group_id, queue_url=None):
        queue_url = queue_url or self.stage_queue_url
        self.sqs.send_message(
            QueueUrl=queue_url, MessageBody=message, MessageGroupId=group_id, MessageDeduplicationId=str(uuid.uuid4())
        )
