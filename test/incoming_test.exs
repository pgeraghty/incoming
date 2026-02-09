defmodule IncomingTest do
  use ExUnit.Case
  doctest Incoming

  test "greets the world" do
    assert Incoming.hello() == :world
  end
end
