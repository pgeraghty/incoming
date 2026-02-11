defmodule Incoming.Message do
  @moduledoc """
  Represents a received inbound email message.
  """

  defstruct id: nil,
            mail_from: nil,
            rcpt_to: [],
            received_at: nil,
            raw_path: nil,
            raw_data: nil,
            meta_path: nil,
            attempts: 0

  @type t :: %__MODULE__{
          id: String.t(),
          mail_from: binary(),
          rcpt_to: [binary()],
          received_at: DateTime.t(),
          raw_path: String.t() | nil,
          raw_data: binary() | nil,
          meta_path: String.t() | nil,
          attempts: non_neg_integer()
        }

  def headers(%__MODULE__{raw_path: path}) when is_binary(path) do
    path
    |> File.read!()
    |> split_headers()
    |> parse_headers()
  end

  def headers(%__MODULE__{raw_data: data}) when is_binary(data) do
    data
    |> split_headers()
    |> parse_headers()
  end

  def headers(%__MODULE__{}), do: %{}

  defp split_headers(content) do
    case String.split(content, "\r\n\r\n", parts: 2) do
      [headers, _body] -> headers
      [headers] -> headers
    end
  end

  defp parse_headers(headers) do
    headers
    |> String.split(~r/\r?\n/, trim: true)
    |> unfold_headers()
    |> Enum.reduce(%{}, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [key, value] ->
          Map.update(acc, String.downcase(String.trim(key)), String.trim(value), fn existing ->
            existing <> ", " <> String.trim(value)
          end)

        _ ->
          acc
      end
    end)
  end

  defp unfold_headers(lines) do
    Enum.reduce(lines, [], fn line, acc ->
      if String.starts_with?(line, " ") or String.starts_with?(line, "\t") do
        case acc do
          [] -> acc
          [prev | rest] -> [prev <> " " <> String.trim(line) | rest]
        end
      else
        [line | acc]
      end
    end)
    |> Enum.reverse()
  end
end
