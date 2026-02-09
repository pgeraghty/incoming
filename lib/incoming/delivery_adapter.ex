defmodule Incoming.DeliveryAdapter do
  @moduledoc """
  Behaviour for delivering messages to application code.
  """

  @type message :: Incoming.Message.t()
  @type result :: :ok | {:retry, reason :: term()} | {:reject, reason :: term()}

  @callback deliver(message) :: result()

  @callback pre_deliver(message) :: :ok
  @callback post_deliver(message, result) :: :ok

  @optional_callbacks [pre_deliver: 1, post_deliver: 2]
end
