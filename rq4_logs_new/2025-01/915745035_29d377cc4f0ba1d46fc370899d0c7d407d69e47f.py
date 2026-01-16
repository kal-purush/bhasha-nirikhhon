from phi.agent import Agent
from phi.model.groq import Groq
from phi.tools.yfinance import YFinanceTools
from phi.tools.duckduckgo import DuckDuckGo
from dotenv import load_dotenv


load_dotenv()

web_agent = Agent(
    name = "web agent",
    model=Groq(id="llama-3.3-70b-versatile"),
    tools=[DuckDuckGo()],
    instructions=["Always include sources."],
    show_tool_calls=True,
    markdown=True
)

finance_agent = Agent(
    model=Groq(id="llama-3.3-70b-versatile"),
    tools=[YFinanceTools(stock_price=True, analyst_recommendations=True,stock_fundamentals=True)],
    show_tool_calls=True,
    markdown=True,
    instructions=["Use tables to display data."],
    debug_mode=True
)

agent_team = Agent(
    model=Groq(id="llama-3.3-70b-versatile"),
    team = [web_agent, finance_agent],
    instructions=["Always include sources", "Use tables to display data."],
    show_tool_calls=True,
    markdown=True,
    debug_mode=True
)

agent_team.print_response("Get the latest news about NVDA", stream=True)