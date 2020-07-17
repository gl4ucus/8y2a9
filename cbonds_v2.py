import pandas as pd
import aiohttp
import asyncio
import re


def get_data(from_issue, to_issue):
  file_name = "cbonds" + str(from_issue) + " - " + str(to_issue) + ".csv"
  column_names = ["Identifier", "Identifier_Status", "Name", "ISIN", "Ticker", "Bond_Status", "Bond_Type",
                  "Coupon_Freq", "Subordinated", "Sinkable", "Perpetual", "Convertible", "Conversion_Terms", "Structured",
                  "Restructuring", "Securitization", "Mortgage", "Trace", "Covered", "Foreign",
                  "CDO", "Sukuk", "Retail", "Supranational", "Green", "Non-Marketable", "Issuer", "SPV_Issuer",
                  "Borrower", "Amount", "Floating", "Ref_Rate", "Margin", "Current_Rate"]

  incomplete_list = [i for i in range(from_issue, to_issue)]
  completed = pd.DataFrame(columns = column_names)


  async def get(identifier):
      async with aiohttp.ClientSession() as session:
          async with session.get("http://cbonds.com/emissions/issue/" + str(identifier)) as response:
              content = await response.text()

              if "404 page is not found" in content:
                  identifier_status = "Invalid"
                  return(identifier, identifier_status)

              identifier_status = "Valid"

              name = re.findall(r'<title>(.*?)</title>', content)
              name = name[0] if len(name) != 0 and len(name[0]) != 0 else None


              isin = re.findall(r'ISIN / ISIN RegS</td><td class="cb_text_right" width="50%">(.*?)</td></tr>',
                                content)
              isin = isin[0] if len(isin) != 0 and len(isin[0]) != 0 else None

              ticker = re.findall(r'Ticker</td><td class="cb_text_right" width="50%">(.*?)<', 
                                  content)
              ticker = ticker[0] if len(ticker) != 0 and len(ticker[0]) != 0 else None


              bond_status = re.findall(r'Issue ratings \(M/S&P/F\)</th></tr></thead><tbody><tr><td>(.*?)</td><td>',
                                      content)
              bond_status = bond_status[0] if len(bond_status) !=0 and len(bond_status[0]) != 0 else None

              bond_type = re.findall(r'Bond type</td><td class="cb_text_right" width="50%">(.*?)<',
                                    content)
              bond_type = bond_type[0] if len(bond_type) != 0 and len(bond_type[0]) != 0 else None

              coupon_freq = re.findall(r'Coupon frequency</td><td class="cb_text_right" width="50%">(.*?)<',
                                      content)
              coupon_freq = coupon_freq[0] if len(coupon_freq) != 0 and len(coupon_freq[0]) != 0 else None


              bond_classification_subordinated = re.findall(
                  r'Subordinated</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_subordinated = bond_classification_subordinated[0] if len(
                  bond_classification_subordinated) != 0 and len(bond_classification_subordinated[0]) != 0 else None


              bond_classification_sinkable = re.findall(
                  r'Sinkable bond</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_sinkable = bond_classification_sinkable[0] if len(
                  bond_classification_sinkable) != 0 and len(bond_classification_sinkable[0]) != 0 else None


              bond_classification_perpetual = re.findall(
                  r'Perpetual</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_perpetual = bond_classification_perpetual[0] if len(
                  bond_classification_perpetual) != 0 and len(bond_classification_perpetual[0]) != 0 else None


              bond_classification_convertible = re.findall(
                  r'Convertible</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_convertible = bond_classification_convertible[0] if len(
                  bond_classification_convertible) != 0 and len(bond_classification_convertible[0]) != 0 else None

              bond_conversion_terms = re.findall(
                  r'Terms of convertion</td><td class="cb_text_right" width="50%">(.*?)<', content)
              bond_conversion_terms = bond_conversion_terms[0] if len(
                  bond_conversion_terms) != 0 and len(bond_conversion_terms[0]) != 0 else None
                
              if bond_conversion_terms is None:
                bond_conversion_terms = re.findall(
                  r'Terms of convertion</div><div class="js_long_box_text cb_hidden">(.*?)<', content)
                bond_conversion_terms = bond_conversion_terms[0] if len(
                  bond_conversion_terms) != 0 and len(bond_conversion_terms[0]) != 0 else None


              bond_classification_structured = re.findall(
                  r'Structured product</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_structured = bond_classification_structured[0] if len(
                  bond_classification_structured) != 0 and len(bond_classification_structured[0]) != 0 else None


              bond_classification_restructuring = re.findall(
                  r'Restructuring</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_restructuring = bond_classification_restructuring[0] if len(
                  bond_classification_restructuring) != 0 and len(bond_classification_restructuring[0]) != 0 else None


              bond_classification_securitisation = re.findall(
                  r'Securitization</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_securitisation = bond_classification_securitisation[0] if len(
                  bond_classification_securitisation) != 0 and len(bond_classification_securitisation[0]) != 0 else None


              bond_classification_mortgage = re.findall(
                  r'Mortgage bonds</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_mortgage = bond_classification_mortgage[0] if len(
                  bond_classification_mortgage) != 0 and  len(bond_classification_mortgage[0]) != 0 else None


              bond_classification_trace = re.findall(
                  r'Trace-eligible</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_trace = bond_classification_trace[0] if len(
                  bond_classification_trace) != 0 and len(bond_classification_trace[0]) != 0 else None


              bond_classification_covered = re.findall(
                  r'Covered</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_covered = bond_classification_covered[0] if len(
                  bond_classification_covered) != 0 and len(bond_classification_covered[0]) != 0 else None


              bond_classification_foreign = re.findall(
                  r'Foreign bonds</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_foreign = bond_classification_foreign[0] if len(
                  bond_classification_foreign) != 0 and len(bond_classification_foreign[0]) != 0 else None


              bond_classification_cdo = re.findall(
                  r'CDO</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_cdo = bond_classification_cdo[0] if len(
                  bond_classification_cdo) != 0 and len(bond_classification_cdo[0]) != 0 else None


              bond_classification_sukuk = re.findall(
                  r'Sukuk</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_sukuk = bond_classification_sukuk[0] if len(
                  bond_classification_sukuk) != 0 and len(bond_classification_sukuk[0]) != 0 else None


              bond_classification_retail = re.findall(
                  r'Retail bonds</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_retail = bond_classification_retail[0] if len(
                  bond_classification_retail) != 0 and len(bond_classification_retail[0]) != 0 else None


              bond_classification_supranational = re.findall(
                  r'Supranational bond issues</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_supranational = bond_classification_supranational[0] if len(
                  bond_classification_supranational) != 0 and len(bond_classification_supranational[0]) != 0 else None


              bond_classification_green = re.findall(
                  r'Green bonds</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_green = bond_classification_green[0] if len(
                  bond_classification_green) != 0 and len(bond_classification_green[0]) != 0 else None


              bond_classification_nonmarketable = re.findall(
                  r'Non-Marketable Securities</td><td class="cb_text_right" width="50%"><span class="(.*?)"', content)
              bond_classification_nonmarketable = bond_classification_nonmarketable[0] if len(
                  bond_classification_nonmarketable) != 0 and len(bond_classification_nonmarketable[0]) != 0 else None


              issuer = re.findall(r'>Issuer</td><td class="cb_text_right" width="50%"><a href=".*?">(.*?)<', content)
              issuer = issuer[0] if len(issuer) != 0 and len(issuer[0]) != 0 else None

              spv_issuer = re.findall(r'>SPV / Issuer</td><td class="cb_text_right" width="50%"><a href=".*?">(.*?)<', content)
              spv_issuer = spv_issuer[0] if len(spv_issuer) != 0 and len(spv_issuer[0]) != 0 else None

              borrower = re.findall(r'>Borrower</td><td class="cb_text_right" width="50%"><a href=".*?">(.*?)<', content)
              borrower = borrower[0] if len(borrower) != 0 and len(borrower[0]) != 0 else None

              amount = re.findall(r'Amount</td><td class="cb_text_right" width="50%">(.*?)<', content)
              amount = amount[0] if len(amount) != 0 and len(amount[0]) != 0 else None
              
            
              floating = re.findall(r'Floating rate</td><td class="cb_text_right" width="50%">(.*?)<', content)
              floating = floating[0] if len(floating) != 0 and len(floating[0]) != 0 else None

              ref_rate = re.findall(r'Reference rate</td><td class="cb_text_right" width="50%">(.*?)<', content)
              ref_rate = ref_rate[0] if len(ref_rate) != 0 and len(ref_rate[0]) != 0 else None

              margin = re.findall(r'Margin</td><td class="cb_text_right" width="50%">(.*?)<', content)
              margin = margin[0] if len(margin) != 0 and len(margin[0]) != 0 else None

              current_rate = re.findall(r'Current coupon rate</td><td class="cb_text_right" width="50%">(.*?)<', content)
              current_rate = current_rate[0] if len(current_rate) != 0 and len(current_rate[0]) != 0 else None

          return (identifier, identifier_status, name, isin, ticker, bond_status, bond_type, coupon_freq, bond_classification_subordinated,
                  bond_classification_sinkable, bond_classification_perpetual, bond_classification_convertible, bond_conversion_terms,
                  bond_classification_structured, bond_classification_restructuring, bond_classification_securitisation,
                  bond_classification_mortgage, bond_classification_trace, bond_classification_covered,
                  bond_classification_foreign, bond_classification_cdo, bond_classification_sukuk,
                  bond_classification_retail, bond_classification_supranational, bond_classification_green,
                  bond_classification_nonmarketable, issuer, spv_issuer, borrower, amount, floating, ref_rate, margin, current_rate)

  while len(incomplete_list) != 0:
      try:
          loop = asyncio.get_event_loop()
          coroutines = [get(_) for _ in incomplete_list]
          results = loop.run_until_complete(asyncio.gather(*coroutines))
          df = pd.DataFrame(results, columns = column_names)
          incomplete_list = df.loc[df["Name"] == "503 Service Temporarily Unavailable"]["Identifier"].tolist()
          completed1 = df.loc[df["Name"] != "503 Service Temporarily Unavailable"]
          completed = pd.concat([completed, completed1])

      except Exception as e:
          print(e)
          continue
  
  completed.replace("cb_cross cb_right", "No", inplace = True)
  completed.replace("cb_ok cb_right", "Yes", inplace = True)
  completed.sort_values(by='Identifier', inplace = True)
  #completed.to_csv(file_name, index = False)
  print("Done")
  return completed

list_of_nums = list(range(480000, 511000, 1000))
df = pd.DataFrame(columns =  ["Identifier", "Identifier_Status", "Name", "ISIN", "Ticker", "Bond_Status", "Bond_Type",
                              "Coupon_Freq", "Subordinated", "Sinkable", "Perpetual", "Convertible", "Conversion_Terms", "Structured",
                              "Restructuring", "Securitization", "Mortgage", "Trace", "Covered", "Foreign",
                              "CDO", "Sukuk", "Retail", "Supranational", "Green", "Non-Marketable", "Issuer", "SPV_Issuer",
                              "Borrower", "Amount", "Floating", "Ref_Rate", "Margin", "Current_Rate"])
for i in range(1, len(list_of_nums)):
  df = pd.concat([df, get_data(list_of_nums[i-1], list_of_nums[i])], ignore_index = True)
  print(df.shape)
  
df.to_csv("cbonds300000 - 340000.csv", index=False)
