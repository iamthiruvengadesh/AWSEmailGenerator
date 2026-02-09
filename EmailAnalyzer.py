from dotenv import load_dotenv
from agents import Agent, Runner, trace, function_tool
from openai.types.responses import ResponseTextDeltaEvent
from typing import Dict
import sendgrid
import os
from sendgrid.helpers.mail import Mail, Email, To, Content
import asyncio
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
load_dotenv(override=True)


@function_tool
def getdatafrommysqlpd(query: str):
    #query='select project_name,region,date,cost from daily_usage_cost_data_by_region where date(create_datetime)=date(now())'
    USER='root'
    PASSWORD='my-secret-pw'
    HOST='localhost'
    DBNAME='aws_billing_data'
    engine = create_engine(
    "mysql+pymysql://{}:{}@{}:3306/{}".format(USER, PASSWORD, HOST, DBNAME)
    )
    df = pd.read_sql(query, engine)
    df["date"] = pd.to_datetime(df["date"])

    out = df.pivot_table(
    index=["project_name", "region"],
    columns="date",
    values="cost",
    aggfunc="first"
    )

# optional – make headers clean
    out.columns = out.columns.strftime("%Y-%m-%d")

    out = out.reset_index()
    return out.to_json()
    print(query)

@function_tool
def querymaxavailabledate() -> str:
    try:
    # Establish the connection
        conn = mysql.connector.connect(
        host="localhost",      # e.g., "localhost" or an IP address
        user="root",  # e.g., "root"
        password="my-secret-pw", # your MySQL password
        database="aws_billing_data"  # the database name
    )


    # You can now create a cursor and execute queries
        cursor = conn.cursor()
        cursor.execute("SELECT max(date) FROM daily_usage_cost_data_by_region")
    
    # Fetch results
        results = cursor.fetchall()
        return(results[0][0])

    except mysql.connector.Error as err:
        return(f"Error: {err}")

    finally:
    # Close the cursor and connection to free resources
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()


@function_tool
def send_html_email(subject: str, html_body: str) -> Dict[str, str]:
    """ Send out an email with the given subject and HTML body to all sales prospects """
    sg = sendgrid.SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))
    from_email = Email("no-reply@thiruvengadesh.in")  # Change to your verified sender
    to_email = To("gthiruvengadesh@gmail.com")  # Change to your recipient
    content = Content("text/html", html_body)
    mail = Mail(from_email, to_email, subject, content).get()
    sg.client.mail.send.post(request_body=mail)
    return {"status": "success"}

instructions1 = "You are a aws cost analyser agent, \
use the tool querymaxavailabledate, it will return max date for which cost data is available.\
from the response you prepare the dates for the month from 1st to max date which you got from tool.\
frame the sql query to select columns project_name,region,date,cost from daily_usage_cost_data_by_region with were clause for column date then pass it to tool getdatafrommysqlpd. \
it will now  return the json data , dont add any addtional information by you, striclty limit the response with what asked for"

email_instructions ="You are an email formatter and sender. You receive the body of an email to be sent. \
You first use the subject_writer tool to write a subject for the email, then use the html_converter tool to convert the body to HTML. \
First Insert Email body: Hi All, Please find below AWS Cost Detail, then insert response from html_converter.\
Dont modify the response from html_converter. process as it is.\
Finally, you use the send_html_email tool to send the email with the subject and HTML body."

subject_instructions = "Generate an email subject line in the format:  AWS Cost Details - YYYY-MM-DD. You MUST call the available querymaxavailabledate tool.Format the returned date as YYYY-MM-DD. Do not hard-code the date.Return only the subject line."

html_instructions = """You are generating a plain-text email body that contains an HTML table.
IMPORTANT:
Preserve the line breaks exactly as shown below.
Do not merge lines.
Do not remove blank lines.

The output MUST follow this exact structure (including empty lines):
Hi,

Please find below <strong>cost details</strong>.<br><br>

<HTML TABLE GENERATED FROM THE JSON>
<br>
<strong>Regards,</strong><br>
Devops Team
<br>
Rules for the HTML table:
- The first column header MUST be "project_name".
- The first cell of the only data row MUST be the value of the JSON field "project_name".
- All date keys inside cost_details MUST be rendered as table column headers (in the same order as in the JSON).
- The remaining cells of the row MUST be the corresponding cost values.
- There MUST be exactly one data row.
- Properly Format the HTML Table with required borders.
- Do NOT add any extra rows.
- Do NOT change any values.
- Do NOT add explanations.
- DO NOT populate the values in Dollar. Keep it plain Integers.
- The HTML must be valid and directly renderable.
"""

subject_writer = Agent(name="Email subject writer", instructions=subject_instructions, model="gpt-4o-mini", tools=[querymaxavailabledate])
subject_tool = subject_writer.as_tool(tool_name="subject_writer", tool_description="Write a subject for a aws cost analyser email")

html_converter = Agent(name="HTML email body converter", instructions=html_instructions, model="gpt-4o-mini")
html_tool = html_converter.as_tool(tool_name="html_converter",tool_description="Convert a text email body to an HTML email body")

emailer_agent = Agent(
    name="Email Manager",
    instructions=email_instructions,
    tools=[subject_tool, html_tool, send_html_email, querymaxavailabledate],
    model="gpt-4o-mini",
    handoff_description="Convert an email to HTML and send it")

aws_cost_agent = Agent(
        name="Professional To analyse the AWS Cost usage and spikes",
        instructions=instructions1,
        tools=[querymaxavailabledate, getdatafrommysqlpd],
        model="gpt-4o-mini"
)

tool1 = aws_cost_agent.as_tool(tool_name="aws_cost_agent", tool_description="Compose Email on AWS Cost and Send Email to Business Team")


aws_ops_manager_instructions="""
You are a AWS Ops Manager at ComplAI. Your goal is to compose email using the aws_cost_agent tools.
 
Follow these steps carefully:
1. Generate Drafts: Use  aws_cost_agent tools to generate  email body. Do not proceed until it is ready.
ensure project_name, is 0th column, region is 1st column, get the dates from result and keep it headers.
  
2. Use the send_email tool to send the  email to the user.
 
Crucial Rules:
- You must use the aws_cost_agent  tools to generate the email body — do not write them yourself.
- You must send FIVE email using the send_email tool — never more than one.
"""




aitools = [tool1]
handoff = [emailer_agent]

message = "Send a cold sales email addressed to 'Dear CEO'"

sales_manager = Agent(
    name="AWS OPS Manager",
    instructions=aws_ops_manager_instructions,
    tools=aitools,
    handoffs=handoff,
    model="gpt-4o-mini")

def test():
    print(getdatafrommysqlpd('select project_name,region,date,cost from daily_usage_cost_data_by_region where date(create_datetime)=date(now())'))


async def maintest():
    result = Runner.run_streamed(aws_cost_agent, input="Professional To analyse the AWS Cost usage and spikes")
    async for event in result.stream_events():
        if event.type == "raw_response_event" and isinstance(event.data, ResponseTextDeltaEvent):
            print(event.data.delta, end="", flush=True)
async def main():
    message = "Just Receive the Data from Agent and send it is as Email, Dont modify the response. Dont alter the response from agents. just process as it is"

    with trace("Automated SDR"):
        await Runner.run(sales_manager, message)    

if __name__ == "__main__":
    asyncio.run(main())
