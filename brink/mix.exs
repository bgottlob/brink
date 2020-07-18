defmodule Brink.Mixfile do
  use Mix.Project

  def project do
    [
      app: :brink,
      version: "0.1.3",
      elixir: "~> 1.7",
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      description: "Elixir GenStage front-end for Redis Streams",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/spawnfest/team-brb/"}
    ]
  end

  defp deps do
    [
      {:flow, "~> 1.0", only: :dev, runtime: false},
      {:dialyxir, "~> 0.5", only: :dev, runtime: false},
      {:ex_doc, "~> 0.19", only: :dev, runtime: false},
      {:gen_stage, "~> 1.0"},
      {:redix, "~> 0.11"}
    ]
  end
end
