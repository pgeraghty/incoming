defmodule Incoming.Policy do
  @moduledoc """
  Behaviour for validating inbound SMTP sessions.
  """

  @type decision :: :ok | {:reject, code :: pos_integer(), message :: String.t()}
  @type phase ::
          :connect
          | :helo
          | :mail_from
          | :rcpt_to
          | :data_start
          | :message_complete

  @callback check(map()) :: decision
end
