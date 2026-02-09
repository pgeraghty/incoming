defmodule Incoming.Message do
  @moduledoc """
  Represents a received inbound email message.
  """

  defstruct id: nil,
            mail_from: nil,
            rcpt_to: [],
            received_at: nil,
            raw_path: nil,
            meta_path: nil,
            attempts: 0

  @type t :: %__MODULE__{
          id: String.t(),
          mail_from: binary(),
          rcpt_to: [binary()],
          received_at: DateTime.t(),
          raw_path: String.t(),
          meta_path: String.t(),
          attempts: non_neg_integer()
        }

  def headers(%__MODULE__{raw_path: path}) do
    path
    |> File.read!()
    |> split_headers()
    |> parse_headers()
  end

  defp split_headers(content) do
    case String.split(content, "\r\n\r\n", parts: 2) do
      [headers, _body] -> headers
      [headers] -> headers
    end
  end

  defp parse_headers(headers) do
    headers
    |> String.split(~r/\r?\n/, trim: true)
    |> Enum.reduce(%{}, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [key, value] ->
          Map.put(acc, String.downcase(String.trim(key)), String.trim(value))

        _ ->
          acc
      end
    end)
  end
end
