defmodule Incoming.Message do
  @moduledoc """
  Represents a received inbound email message.
  """

  defstruct id: nil,
            mail_from: nil,
            rcpt_to: [],
            received_at: nil,
            raw_path: nil,
            meta_path: nil

  @type t :: %__MODULE__{
          id: String.t(),
          mail_from: binary(),
          rcpt_to: [binary()],
          received_at: DateTime.t(),
          raw_path: String.t(),
          meta_path: String.t()
        }
end
