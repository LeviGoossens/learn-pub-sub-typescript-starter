import amqp, {type Channel, type ConfirmChannel} from "amqplib";
import {decode} from "@msgpack/msgpack";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    const ch = await conn.createChannel();

    let options: amqp.Options.AssertQueue | undefined;
    if (queueType === SimpleQueueType.Durable) {
        options = {durable: true, arguments: { "x-dead-letter-exchange": "peril_dlx" }};
    }
    else if (queueType === SimpleQueueType.Transient) {
        options = {autoDelete: true, exclusive: true, arguments: { "x-dead-letter-exchange": "peril_dlx" }};
    }

    const queue = await ch.assertQueue(queueName, options);
    await ch.bindQueue(queueName, exchange, key);

    return [ch, queue];
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);

  await ch.consume(queue.queue, async (message: amqp.ConsumeMessage | null) => {
    if (message === null) {
        return;
    }
    let jsonParsed;
    try {
      jsonParsed = JSON.parse(message.content.toString());
    } catch (err) {
      console.error("Could not unmarshal message:", err);
      return;
    }

    try {
      const result = await handler(jsonParsed);
      switch (result) {
        case AckType.Ack:
          ch.ack(message);
          break;

        case AckType.NackRequeue:
          ch.nack(message, false, true);
          break;

        case AckType.NackDiscard:
          ch.nack(message, false, false);
          break;

        default:
          console.error("Unexpected ack type:");
          return;
      }
    } catch (err) {
      console.error("Error handling message:", err);
      ch.nack(message, false, false);
      return;
    }
  });
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);

  await ch.consume(queue.queue, async (message: amqp.ConsumeMessage | null) => {
    if (message === null) {
      return;
    }
    let decoded;
    let deserialized
    try {
      const buffer = Buffer.from(message.content)
      deserialized = decode(buffer, buffer.byteOffset, buffer.byteLength);
    } catch (err) {
      console.error("Could not unmarshal message:", err);
      return;
    }

    try {
      const result = await handler(deserialized);
      switch (result) {
        case AckType.Ack:
          ch.ack(message);
          break;

        case AckType.NackRequeue:
          ch.nack(message, false, true);
          break;

        case AckType.NackDiscard:
          ch.nack(message, false, false);
          break;

        default:
          console.error("Unexpected ack type:");
          return;
      }
    } catch (err) {
      console.error("Error handling message:", err);
      ch.nack(message, false, false);
      return;
    }
  });
}