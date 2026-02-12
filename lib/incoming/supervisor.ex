defmodule Incoming.Supervisor do
  @moduledoc """
  Top-level supervisor for Incoming.

  You typically don't start this directly. It is started by Incoming's OTP application
  when `:incoming` is part of your supervision tree (for example, in a Phoenix app).

  Child startup order:

  - queue backend
  - rate limiter sweeper
  - disk dead-letter GC (only when using `Incoming.Queue.Disk`)
  - delivery supervisor
  - SMTP listeners
  """

  use Supervisor

  def start_link(_args) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    queue_module = Incoming.Config.queue_module()
    queue_child = {queue_module, Incoming.Config.queue_opts()}

    delivery_child = Incoming.Delivery.Supervisor

    listener_children =
      Incoming.Config.listeners()
      |> Enum.map(&Incoming.Listener.child_spec/1)

    maybe_gc_child =
      if queue_module == Incoming.Queue.Disk do
        Incoming.Queue.DiskGC
      end

    children =
      [
        queue_child,
        Incoming.Policy.RateLimiterSweeper,
        maybe_gc_child,
        delivery_child | listener_children
      ]
      |> Enum.reject(&is_nil/1)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
