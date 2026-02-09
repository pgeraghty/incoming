defmodule Incoming.Envelope do
  @moduledoc false

  defstruct mail_from: nil, rcpt_to: []

  @type t :: %__MODULE__{
          mail_from: binary() | nil,
          rcpt_to: [binary()]
        }
end
