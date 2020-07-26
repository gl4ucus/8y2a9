import pandas as pd
import aiohttp
import asyncio
import datetime
import re

column_names = ["Identifier", "Identifier_Status", "Announcement_Type", "Announcement_Date", "Issue", "Issue_No", "Status", "Country", "Maturity", "Amt", 
                "Company", "Company_No", "Prev_Rating", "Prev_Time", "Curr_Rating", "Curr_Time"]

def get_data(from_issue, to_issue):

    incomplete_list = [i for i in range(from_issue, to_issue)]
    completed = pd.DataFrame(columns=column_names)

    async def get(sem, identifier):
        async with sem:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://cbonds.com/news/item/" + str(identifier)) as response:
                    content = await response.text()

                    identifier_status, announcement_type, announcement_date, issue, issue_no, status, country, maturity, amt, company, company_no, prev_rating, prev_time, curr_rating, curr_time = (None for i in range(15))

                    if "File not found" in content:
                        identifier_status = "Invalid"
                        return [identifier, identifier_status] + [None for _ in range(0, len(column_names) - 2)]

                    if "403 Forbidden" in content:
                        identifier_status = "Retry"
                        return [identifier, identifier_status] + [None for _ in range(0, len(column_names) - 2)]

                    if "Internal Server Error" in content:
                        identifier_status = "Retry"
                        return [identifier, identifier_status] + [None for _ in range(0, len(column_names) - 2)]

                    if "503 Service Temporarily Unavailable" in content:
                        identifier_status = "Retry"
                        return [identifier, identifier_status] + [None for _ in range(0, len(column_names) - 2)]

                    identifier_status = "Valid"
                    
                    
                    title = re.findall(r'<h1 class="cb_innr_ttl"><b>(.*?)</b>', content)
                    title = title[0] if len(title) != 0 and len(title[0]) != 0 else None
                    if title is None:
                      return [identifier, identifier_status, "No Title"] + [None for i in range(len(column_names) - 3)]
                    
                    if "New bond issue:" not in title and "Moody's" not in title and "S&P" not in title and "Fitch" not in title:
                      announcement_type = "Others"
                      return [identifier, identifier_status, announcement_type] + [None for i in range(len(column_names) - 3)]

                    if "New bond issue:" in title:
                      announcement_type = "New Issue"

                      table = re.findall(r'<table class="cb_table emtnt_param cb_indent_top_5">(.*?)</table>', content)
                      
                      if len(table) == 1 and len(table[0]) > 0:
                        table = table[0]
                        table = re.sub('<.*?>', ' ', table)
                        table = table.strip()
                        table = table.split('  ')
                        table = [_ for _ in table if _ != '']
                        if len(table) % 2 == 0:
                          table = dict(zip(table[0:int(len(table)/2)], table[int(len(table)/2):len(table)]))
                          status = table.get("Status")
                          country = table.get('Country of risk')
                          maturity = table.get("Maturity (option)")
                          amt = table.get("Amount")
                          prev_rating = None
                          prev_time = None
                          curr_rating = table.get('Issue ratings (M/S&amp;P/F)')
                          curr_time = str(datetime.datetime.now())


                    elif "Moody's" in title or "S&P" in title or "Fitch" in title:
                      announcement_type = "Update"

                    issue = re.findall(r'Issue:\xa0</b><a href="http://cbonds.com/emissions/issue/.*?">(.*?)<', content)
                    issue = issue[0] if len(issue) != 0 and len(issue[0]) != 0 else None

                    issue_no = re.findall('Issue:\xa0</b><a href="http://cbonds.com/emissions/issue/(.*?)"', content)
                    issue_no = issue_no[0] if len(issue_no) != 0 and len(issue_no[0]) != 0 else None

                    announcement_date = re.findall(r'class="cb_news"><p class="cb_left cb_indent_top_10">(.*?)<b>', content)
                    announcement_date = announcement_date[0] if len(announcement_date) != 0 and len(announcement_date[0]) != 0 else None
                    if announcement_date:
                      announcement_date = announcement_date.replace('|', '')
                    
                    company = re.findall(r'Company: </b><a href="http://cbonds.com/organisations/emitent/.*?">(.*?)<', content)
                    company = company[0] if len(company) != 0 and len(company[0]) != 0 else None

                    company_no = re.findall(r'Company: </b><a href="http://cbonds.com/organisations/emitent/(.*?)"', content)
                    company_no = company_no[0] if len(company_no) != 0 and len(company_no[0]) != 0 else None

                return [identifier, identifier_status, announcement_type, announcement_date, issue, issue_no, status, country, maturity, amt, company, company_no, prev_rating, prev_time, curr_rating, curr_time]

    while len(incomplete_list) != 0:
      try:
        loop = asyncio.get_event_loop()
        sem = asyncio.Semaphore(5)
        coroutines = [get(sem, _) for _ in incomplete_list]
        results = loop.run_until_complete(asyncio.gather(*coroutines))
        df = pd.DataFrame(results, columns=column_names)
        incomplete_list = df.loc[df["Identifier_Status"] == "Retry"]["Identifier"].tolist()
        completed1 = df.loc[df["Identifier_Status"] != "Retry"]
        completed = pd.concat([completed, completed1])
      except Exception as e:
        print(e)
        continue

    print("Done")
    completed.sort_values(by='Identifier', inplace=True)
    return completed

list_of_nums = list(range(0, 101000, 1000))
df = pd.DataFrame(columns= column_names)
for i in range(1, len(list_of_nums)):
    df = pd.concat([df, get_data(list_of_nums[i - 1], list_of_nums[i])], ignore_index=True)
    print(df.shape)
    df.to_csv("100000.csv", index = False)