defmodule Incoming.MixProject do
  use Mix.Project

  def project do
    [
      app: :incoming,
      version: "0.1.0",
      name: "Incoming",
      description: description(),
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      package: package(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Incoming.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Production-grade inbound SMTP server library for Elixir."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/pgeraghty/incoming"}
    ]
  end
end
