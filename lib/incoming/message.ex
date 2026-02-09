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

  def headers(%__MODULE__{raw_path: path}) do
    path
    |> File.stream!([], 1024)
    |> Enum.reduce_while(%{}, fn line, acc ->
      cond do
        line == "\r\n" or line == "\n" ->
          {:halt, acc}

        String.starts_with?(line, " ") or String.starts_with?(line, "\t") ->
          {:cont, acc}

        true ->
          case String.split(line, ":", parts: 2) do
            [key, value] ->
              {:cont, Map.put(acc, String.downcase(String.trim(key)), String.trim(value))}

            _ ->
              {:cont, acc}
          end
      end
    end)
  end
end
