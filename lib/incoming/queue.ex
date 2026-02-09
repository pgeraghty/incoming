defmodule Incoming.Queue do
  @moduledoc """
  Behaviour for queue backends.
  """

  @callback enqueue(
              mail_from :: binary(),
              rcpt_to :: [binary()],
              data :: binary(),
              opts :: keyword()
            ) :: {:ok, Incoming.Message.t()} | {:error, term()}

  @callback enqueue_stream(
              mail_from :: binary(),
              rcpt_to :: [binary()],
              chunks :: Enumerable.t(),
              opts :: keyword()
            ) :: {:ok, Incoming.Message.t()} | {:error, term()}

  @optional_callbacks enqueue_stream: 4

  @callback dequeue() :: {:ok, Incoming.Message.t()} | {:empty}
  @callback ack(message_id :: String.t()) :: :ok
  @callback nack(message_id :: String.t(), :retry | :reject, reason :: term()) :: :ok
  @callback depth() :: non_neg_integer()
  @callback recover() :: :ok
end
