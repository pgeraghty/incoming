defmodule Incoming.Id do
  @moduledoc false

  def generate do
    :crypto.strong_rand_bytes(16)
    |> Base.encode16(case: :lower)
  end
end
