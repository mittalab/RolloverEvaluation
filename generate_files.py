import pandas as pd
import argparse
import re
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

from xlsxwriter.utility import xl_rowcol_to_cell
import calendar

# --- Utility Functions ---

def parse_contract_details(contract_str):
    """
    Parses the CONTRACT_D string (e.g., FUTSTKBHARATFORG28-AUG-2025) to extract the Symbol (Stock Price) and Date.
    
    The expected format is: FUTSTK<Symbol><DD-MMM-YYYY>
    """
    # Regex to capture the stock symbol (group 1) and the date (group 2).
    # Updated to handle the DD-MMM-YYYY date format.
    match = re.search(r'^FUTSTK([A-Z0-9]+)(\d{2}-[A-Z]{3}-\d{4})$', contract_str)
    
    if match:
        symbol = match.group(1)
        date_str = match.group(2)
        try:
            # Parse the date string (e.g., '28-AUG-2025'). Note the hyphens.
            contract_date = datetime.strptime(date_str, '%d-%b-%Y')
            return symbol, contract_date
        except ValueError:
            print(f"Warning: Could not parse date '{date_str}' from {contract_str} using format '%d-%b-%Y'")
            return symbol, None
            
    print(f"Warning: Could not parse contract string {contract_str}. It might not match the expected FUTSTK<SYMBOL><DD-MMM-YYYY> format.")
    return None, None

def generate_last_six_months(input_month_year: str) -> list:
    """
    Generates a list of strings for the last 6 months in MMMYYYY format,
    starting from the month *before* the input month.

    Args:
        input_month_year: A string representing a month and year in MMMYYYY format (e.g., "DEC2023").

    Returns:
        A list of 6 strings, each in MMMYYYY format.
    """
    # 1. Parse the input string into a datetime object
    # We use a default day (like 1) for the parsing to work
    date_obj = datetime.strptime(input_month_year, "%b%Y")

    # 2. Generate the last 6 months
    # The list will contain the 6 months *leading up to* the input month
    # If the user wants to include the input month itself, adjust the range to -5 to 0
    past_months = []
    for i in range(1, 7):
        # Subtract 'i' months from the input date using relativedelta
        # This handles month/year transitions correctly
        previous_date = date_obj - relativedelta(months=i)

        # 3. Format the new date object back into the desired MmmYYYY string format
        formatted_month_year = previous_date.strftime("%b%Y")
        past_months.append(formatted_month_year)

    # The list is generated from most recent to oldest, so reverse it to be chronological
    return past_months[::-1]

def calculate_averages(folder1_path, current_month_symbol_map, curr_month_year):
    """
    Reads the last 6 monthly Rollover Data files from folder1 and calculates 
    the average Rollover% and Rollover Cost for each Symbol.
    """
    historical_data = []

    # return pd.DataFrame({
    #     'Symbol': current_month_symbol_map.keys(),
    #     'Avg. Roll Over': 0.0,
    #     'Avg. Rollover Cost': 0.0
    # })

    # 1. Find and sort historical files
    print(folder1_path)

    prev_months = generate_last_six_months(curr_month_year)
    print(prev_months)
    # print(folder1_path.glob('*_Rollover_Data.csv'))
    # history_files = sorted(
    #     folder1_path.glob('*_Rollover_Data.csv'),
    #     key=lambda p: p.stat().st_mtime, # Sort by file modification time (proxy for creation date)
    #     reverse=True # Newest first
    # )
    #
    # print(history_files)
    # Take the last 6 (newest 6) files
    files_to_average = []
    for monYear in prev_months:
        file = monYear + "_Rollover_Data.csv"
        print(file)
        filePath = list(folder1_path.glob(file))
        print(filePath[0])
        files_to_average.append(filePath[0])


    print(files_to_average)
    if not files_to_average:
        print("No historical rollover files found for averaging. Avg. columns will be 0.")
        # Return a DataFrame of zeros if no history exists
        return pd.DataFrame({
            'Symbol': current_month_symbol_map.keys(),
            'Avg. Roll Over': 0.0,
            'Avg. Rollover Cost': 0.0
        })

    # 2. Read and aggregate data from the selected files
    for file_path in files_to_average:
        try:
            # Only read the Symbol, Rollover%, and Rollover cost columns
            df = pd.read_csv(file_path, usecols=['Symbol', 'Rollover%', 'Rollover cost'], skipinitialspace=True)
            historical_data.append(df)
        except Exception as e:
            print(f"Error reading historical file {file_path.name}: {e}")
            continue

    if not historical_data:
        print("No valid historical data could be loaded. Avg. columns will be 0.")
        return pd.DataFrame({
            'Symbol': current_month_symbol_map.keys(),
            'Avg. Roll Over': 0.0,
            'Avg. Rollover Cost': 0.0
        })
        
    # Concatenate all historical data
    all_history = pd.concat(historical_data, ignore_index=True)
    print("ABC")
    # 3. Group by Symbol and calculate the average
    # Important: This calculates the average across all entries in the 6 files.
    avg_df = all_history.groupby('Symbol').agg(
        {'Rollover%': 'mean', 'Rollover cost': 'mean'}
    ).rename(columns={
        'Symbol': 'Symbol',
        'Rollover%': 'Avg. Roll Over',
        'Rollover cost': 'Avg. Rollover Cost'
    })

    print("START AVG")
    final_avg_df = avg_df.reset_index()
    print(final_avg_df)
    print("DONE AVG")
    return final_avg_df

# --- Main Logic ---

def generate_rollover_report(folder1, folder2, file1_path, file2_path, file3_path, file4_path):
    """
    Main function to process financial files and generate the rollover report.
    """
    print("--- Starting Rollover Report Generation ---")

    # Define paths
    folder1_path = Path(folder1)
    folder2_path = Path(folder2)
    file1_path = Path(file1_path)
    file2_path = Path(file2_path)
    file3_path = Path(file3_path)
    file4_path = Path(file4_path)

    if not folder1_path.exists():
        folder1_path.mkdir(parents=True, exist_ok=True)
        print(f"Created output folder: {folder1}")

    if not folder2_path.exists():
        folder2_path.mkdir(parents=True, exist_ok=True)
        print(f"Created output folder: {folder2}")

    try:
        # 1. Read Futures Data (file1)
        print(f"Reading futures data from {file1_path.name}...")
        futures_df = pd.read_csv(file1_path, skipinitialspace=True)
        
        # Apply the parsing function and create new columns
        futures_df[['Symbol', 'Contract Date']] = futures_df['CONTRACT_D'].apply(
            lambda x: pd.Series(parse_contract_details(x))
        )
        
        futures_df.dropna(subset=['Symbol', 'Contract Date'], inplace=True)
        
        # Identify the month order (Current, Next, Next-to-Next) based on Contract Date
        futures_df.sort_values(['Symbol', 'Contract Date'], inplace=True)
        
        # The first date for any Symbol is the current month contract date
        current_month_dates = futures_df.groupby('Symbol')['Contract Date'].first()
        current_month_names = {
            symbol: date.strftime('%b%Y') 
            for symbol, date in current_month_dates.items()
        }
        
        # Use the most frequent current month name for the output filename
        if not current_month_names:
            print("Error: No valid symbols found in futures data.")
            return
            
        current_month_name = max(set(current_month_names.values()), key=list(current_month_names.values()).count)
        output_filename = f"{current_month_name}_Rollover_Data.csv"
        output_filename_2 = f"{current_month_name}_Rollover_Data.xlsx"

        # --- Futures Calculation Logic ---
        
        # Group by Symbol to perform calculations on the 3-row blocks
        grouped_futures = futures_df.groupby('Symbol')
        
        rollover_results = []
        
        for symbol, group in grouped_futures:
            # Check if we have at least 2 contracts (current and next)
            if len(group) < 2:
                print(f"Skipping {symbol}: Less than 2 contract months available.")
                continue

            # Assuming the sorted group order is: Current, Next, Next-to-Next
            curr = group.iloc[0]
            next_m = group.iloc[1]
            
            # Use next-to-next only if it exists
            next_to_next_m = group.iloc[2] if len(group) > 2 else None

            # 1. Future Price (Next Month's Close Price)
            future_price = next_m['CLOSE_PRIC']

            # 2. Spot (Will be merged later from file2) - Placeholder for now
            # Spot calculation will be done after merging file2 data
            
            # 3. Rollover Cost
            # Formula: (Next Month CLOSE_PRIC - Curr Month CLOSE_PRIC) / (Current Month Spot) * 100
            # Since Spot is unknown here, we use the Current Month CLOSE_PRIC as a temporary stand-in
            # and will correct the division denominator after merging Spot.
            temp_rollover_cost_numerator = next_m['CLOSE_PRIC'] - curr['CLOSE_PRIC']
            
            # 4. Rollover %
            curr_oi = curr['OI_NO_CON']
            next_oi = next_m['OI_NO_CON']
            next_to_next_oi = next_to_next_m['OI_NO_CON'] if next_to_next_m is not None else 0
            
            if (curr_oi + next_oi + next_to_next_oi) == 0:
                rollover_pct = 0.0
            else:
                rollover_pct = (next_oi + next_to_next_oi) / (curr_oi + next_oi + next_to_next_oi) * 100
            
            rollover_results.append({
                'Symbol': symbol,
                'Future Price': future_price,
                'Rollover%': rollover_pct,
                'Temp Rollover Cost Num': temp_rollover_cost_numerator,
                'Curr Month Close': curr['CLOSE_PRIC'], # Used for M_o_M%
            })
            
        if not rollover_results:
            print("Error: No valid rollover calculations could be performed.")
            return

        final_df = pd.DataFrame(rollover_results).set_index('Symbol')
        
        print("Futures calculations completed.")

        # 2. Read Spot Data (file2)
        print(f"Reading spot data from {file2_path.name}...")
        spot_df = pd.read_csv(file2_path, usecols=['SYMBOL', 'CLOSE_PRICE'], skipinitialspace=True)
        spot_df.rename(columns={'CLOSE_PRICE': 'Spot'}, inplace=True)
        spot_df = spot_df[spot_df['SYMBOL'].isin(final_df.index)].set_index('SYMBOL')

        print(f"Reading prev month spot data from {file3_path.name}...")
        prev_spot_df = pd.read_csv(file3_path, usecols=['SYMBOL', 'CLOSE_PRICE'], skipinitialspace=True)
        prev_spot_df.rename(columns={'CLOSE_PRICE': 'PrevMonthSpot'}, inplace=True)
        prev_spot_df = prev_spot_df[prev_spot_df['SYMBOL'].isin(final_df.index)].set_index('SYMBOL')

        print("EHY")
        # Merge spot data into the final results
        final_df = final_df.join(spot_df, how='inner')
        final_df = final_df.join(prev_spot_df, how='inner')
        print("EHY")

        # --- File 4: Sectoral Index Merge (Updated to use left join) ---
        print(f"Reading sectoral index data from {file4_path.name}...")

        # Note: We assume the column name for Sectoral Index in file4 is exactly 'sectoral index'
        sector_df = pd.read_csv(file4_path, usecols=['Sectoral Index', 'Symbol'], skipinitialspace=True)
        sector_df.drop_duplicates(subset=['Symbol'], inplace=True) # Ensure unique symbol for merging

        # Set Symbol as index for joining with final_df
        sector_df.set_index('Symbol', inplace=True)

        # Perform the merge. Use how='left' to keep all symbols from final_df
        # and fill 'sectoral index' with NaN (blank) if not found in file4.
        final_df = final_df.join(sector_df, how='left')

        final_df.reset_index(inplace=True) # Symbol is now a column

        # --- Final Calculations requiring Spot Price ---
        
        # 5. Basis (Current month row)
        # Formula: Future Price (next month close) - Spot (file2 close)
        final_df['Basis'] = final_df['Future Price'] - final_df['Spot']
        
        # 6. Rollover Cost (Corrected)
        # Formula: (Next Month CLOSE_PRIC - Curr Month CLOSE_PRIC) / (Current Month Spot) * 100
        final_df['Rollover cost'] = (final_df['Temp Rollover Cost Num'] / final_df['Spot']) * 100
        final_df.drop(columns=['Temp Rollover Cost Num'], inplace=True)

        # 7. M_o_M%
        # Formula: (CLOSE_PRICE in file2 (Spot) - CLOSE_PRIC in file3 (Prev Month Close)) / (CLOSE_PRIC in file3 (Prev Month Close)) * 100
        final_df['M_o_M%'] = (final_df['Spot'] - final_df['PrevMonthSpot']) / final_df['PrevMonthSpot'] * 100
        final_df.drop(columns=['Curr Month Close', 'PrevMonthSpot'], inplace=True)


        # 3. Read Historical Averages
        print(f"Calculating 6-month historical averages from {folder1}...")
        print(f"current month names {current_month_names}")
        avg_df = calculate_averages(folder2_path, current_month_names, current_month_name)

        # Set Symbol as index for joining with final_df
        print(avg_df)
        avg_df.set_index('Symbol', inplace=True)
        final_df = final_df.rename(columns={'index': 'Symbol'})
        final_df.set_index('Symbol', inplace=True)
        print(avg_df)
        print(final_df)
        # Merge averages into the final results
        final_df = final_df.join(avg_df, how='left').fillna(0)
        print(final_df)
        print("DONE")
        # --- Difference Calculations ---

        # 8. Diff Rollover%
        final_df['Diff Rollover%'] = final_df['Rollover%'] - final_df['Avg. Roll Over']

        # 9. Diff Rollover Cost
        final_df['Diff Rollover Cost'] = final_df['Rollover cost'] - final_df['Avg. Rollover Cost']

        # 10. Sort the final data: first by sectoral index, then by symbol
        final_df.sort_values(by=['Sectoral Index', 'Symbol'], inplace=True)

        # List of columns to be rounded
        rounding_cols = [
            'Spot', 'M_o_M%', 'Future Price', 'Basis', 'Rollover%',
            'Avg. Roll Over', 'Rollover cost', 'Avg. Rollover Cost',
            'Diff Rollover%', 'Diff Rollover Cost'
        ]

        # Round the numerical columns to 2 decimal places
        final_df[rounding_cols] = final_df[rounding_cols].round(2)

        # --- Final Output ---

        # Reorder and rename columns to match the requested output
        final_df.reset_index(inplace=True)
        
        print(final_df)
        output_columns = [
            'Sectoral Index', 'Symbol', 'Spot', 'Future Price', 'Basis', 'Rollover%',
            'Avg. Roll Over', 'Rollover cost', 'Avg. Rollover Cost', 
            'Diff Rollover%', 'Diff Rollover Cost', 'M_o_M%'
        ]

        print("DONE2")
        final_df = final_df[output_columns]
        print("DONE3")

        output_path = folder2_path / output_filename
        final_df.to_csv(output_path, index=False, float_format='%.2f')
        print(f"\nSuccessfully generated report: {output_filename}")
        print(f"Output saved to: {output_path.resolve()}")

        output_path_2 = folder1_path / output_filename_2
        # Create a Pandas ExcelWriter object using the xlsxwriter engine
        try:
            writer = pd.ExcelWriter(output_path_2, engine='xlsxwriter')
            # Write the DataFrame to a specific sheet
            final_df.to_excel(writer, sheet_name='Rollover Data', index=False, float_format='%.2f', startrow=4)

            # Get the workbook and worksheet objects
            workbook  = writer.book
            worksheet = writer.sheets['Rollover Data']

            # --- Filtering and Writing to New Sheets ---

            # 1. Long Rolls
            long_rolls_df = final_df[(final_df['M_o_M%'] > 0) & (final_df['Diff Rollover%'] > 0) & (final_df['Diff Rollover Cost'] > 0)]
            long_rolls_df.to_excel(writer, sheet_name='Long Rolls', index=False, float_format='%.2f', startrow=4)
            worksheet_lr = writer.sheets['Long Rolls']
            # worksheet_lr.write(0, 0, "Long Rolls (MoM+ , %Roll+ , Cost+)", green_format)
            # worksheet_lr.write(1, 0, "Short Rolls (MoM- , %Roll+ , Cost-)", red_format)
            # worksheet_lr.write(2, 0, "Short Covering (MoM+ , %Roll- , Cost+)", light_green_format)
            # worksheet_lr.write(3, 0, "Long Unwind (MoM- , %Roll- , Cost-)", light_red_format)
            # worksheet_lr.freeze_panes(5, 2)


            # 2. Short Rolls
            short_rolls_df = final_df[(final_df['M_o_M%'] < 0) & (final_df['Diff Rollover%'] > 0) & (final_df['Diff Rollover Cost'] < 0)]
            short_rolls_df.to_excel(writer, sheet_name='Short Rolls', index=False, float_format='%.2f', startrow=4)
            worksheet_sr = writer.sheets['Short Rolls']
            # worksheet_sr.write(0, 0, "Long Rolls (MoM+ , %Roll+ , Cost+)", green_format)
            # worksheet_sr.write(1, 0, "Short Rolls (MoM- , %Roll+ , Cost-)", red_format)
            # worksheet_sr.write(2, 0, "Short Covering (MoM+ , %Roll- , Cost+)", light_green_format)
            # worksheet_sr.write(3, 0, "Long Unwind (MoM- , %Roll- , Cost-)", light_red_format)
            # worksheet_sr.freeze_panes(5, 2)

            # 3. Short Covering
            short_covering_df = final_df[(final_df['M_o_M%'] > 0) & (final_df['Diff Rollover%'] < 0) & (final_df['Diff Rollover Cost'] > 0)]
            short_covering_df.to_excel(writer, sheet_name='Short Covering', index=False, float_format='%.2f', startrow=4)
            worksheet_sc = writer.sheets['Short Covering']
            # worksheet_sc.write(0, 0, "Long Rolls (MoM+ , %Roll+ , Cost+)", green_format)
            # worksheet_sc.write(1, 0, "Short Rolls (MoM- , %Roll+ , Cost-)", red_format)
            # worksheet_sc.write(2, 0, "Short Covering (MoM+ , %Roll- , Cost+)", light_green_format)
            # worksheet_sc.write(3, 0, "Long Unwind (MoM- , %Roll- , Cost-)", light_red_format)
            # worksheet_sc.freeze_panes(5, 2)


            # 4. Long Unwind
            long_unwind_df = final_df[(final_df['M_o_M%'] < 0) & (final_df['Diff Rollover%'] < 0) & (final_df['Diff Rollover Cost'] < 0)]
            long_unwind_df.to_excel(writer, sheet_name='Long Unwind', index=False, float_format='%.2f', startrow=4)
            worksheet_lu = writer.sheets['Long Unwind']
            # worksheet_lu.write(0, 0, "Long Rolls (MoM+ , %Roll+ , Cost+)", green_format)
            # worksheet_lu.write(1, 0, "Short Rolls (MoM- , %Roll+ , Cost-)", red_format)
            # worksheet_lu.write(2, 0, "Short Covering (MoM+ , %Roll- , Cost+)", light_green_format)
            # worksheet_lu.write(3, 0, "Long Unwind (MoM- , %Roll- , Cost-)", light_red_format)
            # worksheet_lu.freeze_panes(5, 2)

            apply_worksheet_formatting (final_df, worksheet, workbook)
            apply_worksheet_formatting (long_rolls_df, worksheet_lr, workbook)
            apply_worksheet_formatting (short_rolls_df, worksheet_sr, workbook)
            apply_worksheet_formatting (short_covering_df, worksheet_sc, workbook)
            apply_worksheet_formatting (long_unwind_df, worksheet_lu, workbook)

            # Close the Pandas Excel writer and output the Excel file.
            writer.close()
            print(f"\nSuccessfully generated report: {output_filename_2}")
            print(f"Output saved to: {output_path_2.resolve()}")

        except ImportError:
            # Fallback to CSV if xlsxwriter is not available
            print("Warning: 'xlsxwriter' not found. Falling back to CSV without conditional formatting.")
            output_filename = f"{current_month_name}_Rollover_Data.csv"
            output_path = folder1_path / output_filename
            final_df.to_csv(output_path, index=False, float_format='%.2f')
        
    except FileNotFoundError as e:
        print(f"Error: One of the input files was not found. Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during processing: {e}")

# def add_legend(filename, legend_items):
#     """
#     Adds a color-coded legend to the top of an Excel worksheet.
#
#     Args:
#         filename (str): The path to the Excel file.
#         legend_items (dict): A dictionary where keys are color hex codes (e.g., 'FF0000' for red)
#                              and values are the descriptions for the legend.
#     """
#     # Load the workbook and get the active sheet
#     wb = openpyxl.load_workbook(filename)
#     sheet = wb.active
#
#     # Determine the number of rows needed for the legend (title row + items)
#     num_legend_rows = len(legend_items) + 2 # Add 2 for a title and spacing
#
#     # Insert blank rows at the top (before row 1) to make space for the legend
#     sheet.insert_rows(1, amount=num_legend_rows)
#
#     # Add a title for the legend
#     sheet.cell(row=1, column=1, value="Legend: Significance of Colors")
#     # Optional: Make the title bold or format as needed
#
#     # Start writing legend items from the third row
#     row_num = 3
#     for color_hex, description in legend_items.items():
#         # Cell for the color block
#         color_cell = sheet.cell(row=row_num, column=1, value="")
#         # Apply the color fill. The 'FF' prefix is often needed for full opacity.
#         fill = PatternFill(start_color=color_hex, end_color=color_hex, fill_type="solid")
#         color_cell.fill = fill
#
#         # Cell for the description
#         description_cell = sheet.cell(row=row_num, column=2, value=description)
#
#         row_num += 1
#
#     # Save the changes to the workbook
#     wb.save(filename)
#     print(f"Legend added to {filename}")

def apply_worksheet_formatting(final_df, worksheet, workbook):
    # --- AUTO-FIT COLUMN WIDTHS ---
    # Set column widths based on the maximum length of the data in each column
    for i, col in enumerate(final_df.columns):
        # Calculate the max length of data + 2 for a little padding
        # If the column is empty, max_len will be 0, so we default to header length.
        try:
            # Get the maximum length of the string representation of all items in the column
            max_len = max(final_df[col].astype(str).str.len().max(), len(col)) + 2
        except:
            # Fallback if the column contains un-stringable data or is empty
            max_len = len(col) + 2
            # Constrain max width to prevent extremely wide columns
        max_len = min(max_len, 50)

        # if i == 0:
        #     max_len = max_len + 5

        # Set the width for the current column (i, i are start and end columns)
        worksheet.set_column(i, i, max_len)

    worksheet.set_column(0, 0, 35)

    # Define the green format for highlighting
    green_format = workbook.add_format({
        'bg_color': '#C6EFCE',
        'font_color': '#006100'
    })
    light_green_format = workbook.add_format({
        'bg_color': '#EEFBF0',
        'font_color': '#006100'
    })

    # Define the red format for highlighting
    red_format = workbook.add_format({
        'bg_color': '#FFC7CE',
        'font_color': '#9C0006'
    })
    light_red_format = workbook.add_format({
        'bg_color': '#FFEBF0',
        'font_color': '#9C0006'
    })

    worksheet.freeze_panes(5, 2)
    worksheet.write(0, 0, "Long Rolls (MoM+ , %Roll+ , Cost+)", green_format)
    worksheet.write(1, 0, "Short Rolls (MoM- , %Roll+ , Cost-)", red_format)
    worksheet.write(2, 0, "Short Covering (MoM+ , %Roll- , Cost+)", light_green_format)
    worksheet.write(3, 0, "Long Unwind (MoM- , %Roll- , Cost-)", light_red_format)

    # Define the conditional formatting rule
    # Data starts from row 2 (header in row 1).
    # Columns (1-indexed for Excel):
    # M_o_M% is column C (index 2)
    # Diff Rollover% is column J (index 9)
    # Diff Rollover Cost is column K (index 10)

    max_row = len(final_df)
    max_col = len(final_df.columns)
    # The range covers all data cells (from A6 to the last data cell)
    data_range = f'A6:{xl_rowcol_to_cell(max_row+4, max_col - 1)}'

    # The formula checks cell values in the *current* row (relative to A2)
    formula_green = '=AND($L6>0, $J6>0, $K6>0)'
    formula_red = '=AND($L6<0, $J6>0, $K6<0)'
    formula_light_green = '=AND($L6>0, $J6<0, $K6>0)'
    formula_light_red = '=AND($L6<0, $J6<0, $K6<0)'

    # Apply the conditional format to the entire data range
    worksheet.conditional_format(data_range, {
        'type': 'formula',
        'criteria': formula_green,
        'format': green_format
    })
    worksheet.conditional_format(data_range, {
        'type': 'formula',
        'criteria': formula_red,
        'format': red_format
    })
    worksheet.conditional_format(data_range, {
        'type': 'formula',
        'criteria': formula_light_red,
        'format': light_red_format
    })
    worksheet.conditional_format(data_range, {
        'type': 'formula',
        'criteria': formula_light_green,
        'format': light_green_format
    })

def get_last_weekday_of_month(year, month, weekday):
    """
    Calculates the date of the last specified weekday (0=Mon, 6=Sun) of a given month.
    """
    # Get the number of days in the month
    _, num_days = calendar.monthrange(year, month)

    # Start checking from the last day of the month
    for day in range(num_days, num_days - 7, -1):
        target_date = datetime(year, month, day)
        # Check if the day is the target weekday
        if target_date.weekday() == weekday:
            return target_date
    return None

def calculate_expiry_date(year, month):
    """
    Calculates the expiry date (last Thursday or last Tuesday) for a given month/year.
    """
    # September 2025 is 2025-09-01
    sept_2025 = datetime(2025, 9, 1)

    current_month_start = datetime(year, month, 1)

    if current_month_start < sept_2025:
        # Before September 2025: Last Thursday (Thursday is 3 in Python's calendar: 0=Mon, 6=Sun)
        weekday_to_find = calendar.THURSDAY  # 3
    else:
        # September 2025 onwards: Last Tuesday (Tuesday is 1)
        weekday_to_find = calendar.TUESDAY   # 1

    expiry_date = get_last_weekday_of_month(year, month, weekday_to_find)

    if expiry_date is None:
        raise ValueError(f"Could not calculate expiry date for {year}-{month:02d}")

    current_datetime = datetime.now()

    if current_datetime < expiry_date:
        raise ValueError(f"Current date {current_datetime} is less than expiry date for {expiry_date}")

    return expiry_date

def get_curr_and_prev_month_dates(input_month_year):
    """
    Parses the input MMMYY string, calculates current and previous month's expiry dates,
    and returns them in DDMMYY format, with an optional DDMMYYYY fallback.
    """
    if input_month_year:
        # Parse the input string (e.g., 'DEC-25')
        try:
            current_month_dt = datetime.strptime(input_month_year, '%b%y')
        except ValueError:
            raise ValueError("Input format must be MMMYY (e.g., DEC25).")
    else:
        # Use current month and year if input is empty
        current_month_dt = datetime.now()

    curr_year = current_month_dt.year
    curr_month = current_month_dt.month

    # Calculate Current Month Date
    curr_month_expiry = calculate_expiry_date(curr_year, curr_month)
    curr_month_date_6 = curr_month_expiry.strftime('%d%m%y')
    curr_month_date_8 = curr_month_expiry.strftime('%d%m%Y')


    # Calculate Previous Month Date
    # Go back one month
    if curr_month == 1:
        prev_month = 12
        prev_year = curr_year - 1
    else:
        prev_month = curr_month - 1
        prev_year = curr_year

    prev_month_expiry = calculate_expiry_date(prev_year, prev_month)
    prev_month_date_6 = prev_month_expiry.strftime('%d%m%y')
    prev_month_date_8 = prev_month_expiry.strftime('%d%m%Y')

    return {
        'curr_6': curr_month_date_6,
        'curr_8': curr_month_date_8,
        'prev_6': prev_month_date_6,
        'prev_8': prev_month_date_8,
        'current_month_name': curr_month_expiry.strftime('%b%Y')
    }

def try_file_read(file_path_6, file_path_8, file_type):
    """Tries to read or generate a file using DDMMYY, falls back to DDMMYYYY."""
    path_6 = Path(file_path_6)
    path_8 = Path(file_path_8)

    # 1. Try to read the file in DDMMYY format
    if path_6.exists():
        return str(path_6)

    # 2. Try to read the file in DDMMYYYY format
    elif path_8.exists():
        return str(path_8)

    # 3. If neither exists, generate mock data (for demonstration purposes)
    else:
        raise FileNotFoundError(f"Could not generate or find file for type {file_type} at {path_6} or {path_8}")

if __name__ == '__main__':
    # parser = argparse.ArgumentParser(
    #     description="Generates a Stock Rollover Data report by processing futures and spot CSV files."
    # )
    #
    # parser.add_argument(
    #     'folder1',
    #     type=str,
    #     help='The path to the output folder (where historical files are read and the new file is written).'
    # )
    # parser.add_argument(
    #     'file1',
    #     type=str,
    #     help='The path to the futures data CSV file (CONTRACT_D, Stock Price, CLOSE_PRIC, OI_NO_CON, etc.).'
    # )
    # parser.add_argument(
    #     'file2',
    #     type=str,
    #     help='The path to the current spot data CSV file (used for Spot and M_o_M% calculation).'
    # )
    # parser.add_argument(
    #     'file3',
    #     type=str,
    #     help='The path to the previous day spot data CSV file (included as per request, but not explicitly used in calculations based on the provided formulas).'
    # )
    # parser.add_argument( # NEW ARGUMENT
    #     'file4',
    #     type=str,
    #     help='The path to the sectoral index data CSV file (used to retrieve sectoral index for sorting and output).'
    # )
    #
    # args = parser.parse_args()

    parser = argparse.ArgumentParser(
        description="Generates mock data and a Stock Rollover Data report based on a month/year input."
    )

    parser.add_argument(
        'month_year',
        type=str,
        nargs='?', # Makes the argument optional
        default='',
        help='The month and year (MMMYY) for the report (e.g., DEC25). Defaults to current month/year.'
    )

    args = parser.parse_args()

    # 1. Calculate dates and month name
    try:
        dates = get_curr_and_prev_month_dates(args.month_year)
    except ValueError as e:
        print(f"Error processing month/year input: {e}")
        exit()

    curr_date_6, curr_date_8 = dates['curr_6'], dates['curr_8']
    prev_date_6, prev_date_8 = dates['prev_6'], dates['prev_8']

    # 2. Define standard folder and file names
    folder1 = "generated_data"
    folder2 = "generated_csv_data"

    # Define file paths for DDMMYY and DDMMYYYY
    file1_6 = f"fo_data/fo{curr_date_6}.csv"
    file2_6 = f"equity_data/sec_bhavdata_full_{curr_date_6}.csv"
    file3_6 = f"equity_data/sec_bhavdata_full_{prev_date_6}.csv"

    file1_8 = f"fo_data/fo{curr_date_8}.csv"
    file2_8 = f"equity_data/sec_bhavdata_full_{curr_date_8}.csv"
    file3_8 = f"equity_data/sec_bhavdata_full_{prev_date_8}.csv"

    file4 = f"index.csv" # File 4 does not use date

    # 3. Generate or find the required files
    print("\n--- Generating/Locating Input Files ---")

    # File 1 (Futures) uses DDMMYY or DDMMYYYY format
    file1_resolved = try_file_read(file1_6, file1_8, 'file1')

    # File 2 (Current Spot) uses DDMMYY or DDMMYYYY format
    file2_resolved = try_file_read(file2_6, file2_8, 'file2')

    # File 3 (Previous Spot) uses DDMMYY or DDMMYYYY format
    file3_resolved = try_file_read(file3_6, file3_8, 'file3')

    file4_resolved = Path(file4)

    print(file1_resolved)
    print(file2_resolved)
    print(file3_resolved)
    print(file4_resolved)

    generate_rollover_report(folder1, folder2, file1_resolved, file2_resolved, file3_resolved, file4_resolved)
