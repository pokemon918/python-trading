import dearpygui.dearpygui as dpg
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, time, timedelta
import pandas as pd
import platform
import os

import sys

module_directory = '..'
sys.path.append(module_directory)
from constants import Reoccurring_day
import settings

# Constants
TIME_FORMAT = "%H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = f"{DATE_FORMAT} {TIME_FORMAT}"


class AcceptableGapsManager:
    def __init__(self):
        self.gaps_data = []
        self.selected_row = -1

        # Create a list of reoccurring day options from the enum
        self.reoccur_day_options = []
        for day in Reoccurring_day:
            self.reoccur_day_options.append(day.name)

    def connect_db(self):
        """Establishes a connection to the database."""
        try:
            conn = psycopg2.connect(host=settings.host,
                                    database=settings.strategies_database,
                                    user=settings.user,
                                    password=settings.password)
            return conn
        except Exception as e:
            dpg.set_value("status", f"Connection Error: {str(e)}")
            return None

    def load_data(self):
        """Loads data from the acceptable_gaps table."""
        conn = self.connect_db()
        if not conn:
            return

        try:
            cursor = conn.cursor()
            query = """
                SELECT acceptable_gaps_id, market, start_datetime, end_datetime, 
                       description, reoccur_day_of_week, reoccur_start_time, reoccur_end_time 
                FROM acceptable_gaps
                ORDER BY market, start_datetime
            """
            cursor.execute(query)
            self.gaps_data = cursor.fetchall()

            # Update the table
            self.update_table_display()

            dpg.set_value("status", f"Loaded {len(self.gaps_data)} records")
            cursor.close()
        except Exception as e:
            dpg.set_value("status", f"Load Error: {str(e)}")
            import traceback
            print(f"Error details: {traceback.format_exc()}")
        finally:
            conn.close()

    def row_clicked(self, sender, app_data, user_data):
        """Handle row click event using the user_data parameter containing the row tag."""
        try:
            # The user_data contains the row tag (e.g., "row_5")
            if user_data and user_data.startswith("row_"):
                row_idx = int(user_data[4:])  # Extract the index number
                self.select_row(row_idx)
        except (ValueError, TypeError) as e:
            print(f"Error parsing row index: {e}")

    def update_table_display(self):
        """Updates the table display with the current data."""
        # Clear existing rows
        if dpg.does_item_exist("gaps_table"):
            dpg.delete_item("gaps_table")

        # Make sure the parent container exists
        if not dpg.does_item_exist("table_container"):
            dpg.set_value("status", "Error: Table container not found")
            return

        # Create the table
        with dpg.table(header_row=True, policy=dpg.mvTable_SizingStretchProp,
                       borders_outerH=True, borders_innerV=True, borders_innerH=True, borders_outerV=True,
                       tag="gaps_table", parent="table_container"):

            # Add special column for selection button
            dpg.add_table_column(label="", width_fixed=True, width=30)

            # Add regular data columns
            dpg.add_table_column(label="ID")
            dpg.add_table_column(label="Market")
            dpg.add_table_column(label="Start DateTime")
            dpg.add_table_column(label="End DateTime")
            dpg.add_table_column(label="Description")
            dpg.add_table_column(label="Reoccur Day")
            dpg.add_table_column(label="Reoccur Start")
            dpg.add_table_column(label="Reoccur End")

            # Add rows - only if we have data
            if self.gaps_data:
                for i, row in enumerate(self.gaps_data):
                    # Create a table row with a label to store the index
                    row_tag = f"row_{i}"
                    with dpg.table_row(label=row_tag, tag=row_tag):
                        # Add selection button in the first column
                        with dpg.table_cell():
                            dpg.add_button(label="", width=25, height=20, callback=self.row_clicked,
                                           tag=f"btn_{i}", user_data=row_tag)

                        # Add remaining cells with data
                        for j, cell in enumerate(row):
                            # Format datetime and time values
                            if j in (2, 3) and cell:  # start_datetime and end_datetime
                                cell_text = cell.strftime(DATETIME_FORMAT)
                            elif j in (6, 7) and cell:  # reoccur_start_time and reoccur_end_time
                                cell_text = cell.strftime(TIME_FORMAT)
                            elif j == 5 and cell is not None:  # reoccur_day_of_week as integer from enum
                                try:
                                    # Convert integer to enum name
                                    cell_text = Reoccurring_day(cell).name
                                except (ValueError, TypeError):
                                    cell_text = str(cell)
                            else:
                                cell_text = str(cell) if cell is not None else ""

                            # Create cell
                            with dpg.table_cell():
                                # Create text
                                text_tag = f"cell_{i}_{j}"
                                dpg.add_text(cell_text, tag=text_tag)

    def select_row(self, row_idx):
        """Handles row selection in the table."""
        self.selected_row = row_idx        

        if 0 <= row_idx < len(self.gaps_data):
            row = self.gaps_data[row_idx]

            # Update form fields with the selected row's data
            dpg.set_value("market", row[1])

            # Handle datetime fields
            if row[2]:  # start_datetime
                start_date = row[2].strftime(DATE_FORMAT)
                start_time = row[2].strftime(TIME_FORMAT)
                dpg.set_value("start_date", start_date)
                dpg.set_value("start_time", start_time)

            if row[3]:  # end_datetime
                end_date = row[3].strftime(DATE_FORMAT)
                end_time = row[3].strftime(TIME_FORMAT)
                dpg.set_value("end_date", end_date)
                dpg.set_value("end_time", end_time)

            dpg.set_value("description", row[4] if row[4] else "")

            # Handle reoccur fields - set the enum name directly
            reoccur_day_value = row[5]
            if reoccur_day_value is None:
                dpg.set_value("reoccur_day", "NotReoccurring")
            else:
                try:
                    # Get the enum name and set it directly
                    enum_name = Reoccurring_day(reoccur_day_value).name
                    dpg.set_value("reoccur_day", enum_name)
                except (ValueError, IndexError, TypeError):
                    dpg.set_value("reoccur_day", "NotReoccurring")

            if row[6]:  # reoccur_start_time
                dpg.set_value("reoccur_start_time", row[6].strftime(TIME_FORMAT))
            else:
                dpg.set_value("reoccur_start_time", "00:00:00")

            if row[7]:  # reoccur_end_time
                dpg.set_value("reoccur_end_time", row[7].strftime(TIME_FORMAT))
            else:
                dpg.set_value("reoccur_end_time", "00:00:00")

            dpg.set_value("status", f"Selected record ID: {row[0]}")
        else:
            self.clear_form()

    def clear_form(self):
        """Clears the input form."""
        dpg.set_value("market", "")
        dpg.set_value("start_date", datetime.now().strftime(DATE_FORMAT))
        dpg.set_value("start_time", "00:00:00")
        dpg.set_value("end_date", datetime.now().strftime(DATE_FORMAT))
        dpg.set_value("end_time", "00:00:00")
        dpg.set_value("description", "")
        dpg.set_value("reoccur_day", "NotReoccurring")  # Use name directly
        dpg.set_value("reoccur_start_time", "00:00:00")
        dpg.set_value("reoccur_end_time", "00:00:00")

    def add_record(self):
        """Adds a new record to the database."""
        conn = self.connect_db()
        if not conn:
            return

        try:
            # Get values from the form
            market = dpg.get_value("market")
            if not market:
                dpg.set_value("status", "Error: Market is required")
                return

            # Parse start datetime
            try:
                start_date_str = dpg.get_value("start_date")
                start_time_str = dpg.get_value("start_time")
                start_datetime = datetime.strptime(f"{start_date_str} {start_time_str}", DATETIME_FORMAT)
            except ValueError:
                dpg.set_value("status", "Error: Invalid start date/time format")
                return

            # Parse end datetime
            try:
                end_date_str = dpg.get_value("end_date")
                end_time_str = dpg.get_value("end_time")
                end_datetime = datetime.strptime(f"{end_date_str} {end_time_str}", DATETIME_FORMAT)
            except ValueError:
                dpg.set_value("status", "Error: Invalid end date/time format")
                return

            description = dpg.get_value("description")

            # Handle reoccurring settings - get the enum name directly from combobox
            reoccur_day_name = dpg.get_value("reoccur_day")

            # Get the enum value from the name
            reoccur_day_value = Reoccurring_day[reoccur_day_name].value

            # Parse reoccur times
            reoccur_start_time = None
            reoccur_end_time = None
            try:
                if dpg.get_value("reoccur_start_time"):
                    time_obj = datetime.strptime(dpg.get_value("reoccur_start_time"), TIME_FORMAT).time()
                    reoccur_start_time = time_obj
            except ValueError:
                dpg.set_value("status", "Error: Invalid reoccur start time format")
                return

            try:
                if dpg.get_value("reoccur_end_time"):
                    time_obj = datetime.strptime(dpg.get_value("reoccur_end_time"), TIME_FORMAT).time()
                    reoccur_end_time = time_obj
            except ValueError:
                dpg.set_value("status", "Error: Invalid reoccur end time format")
                return

            cursor = conn.cursor()
            query = """
                INSERT INTO acceptable_gaps 
                (market, start_datetime, end_datetime, description, 
                 reoccur_day_of_week, reoccur_start_time, reoccur_end_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING acceptable_gaps_id
            """

            values = (
                market, start_datetime, end_datetime, description,
                reoccur_day_value, reoccur_start_time, reoccur_end_time
            )

            cursor.execute(query, values)
            new_id = cursor.fetchone()[0]
            conn.commit()

            dpg.set_value("status", f"Added new record with ID: {new_id}")

            # Reload data to update the display
            self.load_data()
            cursor.close()
        except Exception as e:
            conn.rollback()
            dpg.set_value("status", f"Add Error: {str(e)}")
            import traceback
            print(f"Add Error details: {traceback.format_exc()}")
        finally:
            conn.close()

    def update_record(self):
        """Updates the selected record in the database."""
        if self.selected_row < 0 or self.selected_row >= len(self.gaps_data):
            dpg.set_value("status", "No record selected")
            return

        conn = self.connect_db()
        if not conn:
            return

        try:
            # Get record ID
            record_id = self.gaps_data[self.selected_row][0]

            # Get values from the form
            market = dpg.get_value("market")
            if not market:
                dpg.set_value("status", "Error: Market is required")
                return

            # Parse start datetime
            try:
                start_date_str = dpg.get_value("start_date")
                start_time_str = dpg.get_value("start_time")
                start_datetime = datetime.strptime(f"{start_date_str} {start_time_str}", DATETIME_FORMAT)
            except ValueError:
                dpg.set_value("status", "Error: Invalid start date/time format")
                return

            # Parse end datetime
            try:
                end_date_str = dpg.get_value("end_date")
                end_time_str = dpg.get_value("end_time")
                end_datetime = datetime.strptime(f"{end_date_str} {end_time_str}", DATETIME_FORMAT)
            except ValueError:
                dpg.set_value("status", "Error: Invalid end date/time format")
                return

            description = dpg.get_value("description")

            # Handle reoccurring settings - get the enum name directly from combobox
            reoccur_day_name = dpg.get_value("reoccur_day")

            # Get the enum value from the name
            reoccur_day_value = Reoccurring_day[reoccur_day_name].value

            # Parse reoccur times
            reoccur_start_time = None
            reoccur_end_time = None
            try:
                if dpg.get_value("reoccur_start_time"):
                    time_obj = datetime.strptime(dpg.get_value("reoccur_start_time"), TIME_FORMAT).time()
                    reoccur_start_time = time_obj
            except ValueError:
                dpg.set_value("status", "Error: Invalid reoccur start time format")
                return

            try:
                if dpg.get_value("reoccur_end_time"):
                    time_obj = datetime.strptime(dpg.get_value("reoccur_end_time"), TIME_FORMAT).time()
                    reoccur_end_time = time_obj
            except ValueError:
                dpg.set_value("status", "Error: Invalid reoccur end time format")
                return

            cursor = conn.cursor()
            query = """
                UPDATE acceptable_gaps
                SET market = %s, start_datetime = %s, end_datetime = %s, description = %s,
                    reoccur_day_of_week = %s, reoccur_start_time = %s, reoccur_end_time = %s
                WHERE acceptable_gaps_id = %s
            """

            values = (
                market, start_datetime, end_datetime, description,
                reoccur_day_value, reoccur_start_time, reoccur_end_time, record_id
            )

            cursor.execute(query, values)
            conn.commit()

            dpg.set_value("status", f"Updated record ID: {record_id}")

            # Reload data to update the display
            self.load_data()
            cursor.close()
        except Exception as e:
            conn.rollback()
            dpg.set_value("status", f"Update Error: {str(e)}")
            import traceback
            print(f"Update Error details: {traceback.format_exc()}")
        finally:
            conn.close()

    def delete_record(self):
        """Deletes the selected record from the database."""
        if self.selected_row < 0 or self.selected_row >= len(self.gaps_data):
            dpg.set_value("status", "No record selected")
            return

        conn = self.connect_db()
        if not conn:
            return

        try:
            # Get record ID
            record_id = self.gaps_data[self.selected_row][0]

            cursor = conn.cursor()
            query = "DELETE FROM acceptable_gaps WHERE acceptable_gaps_id = %s"

            cursor.execute(query, (record_id,))
            conn.commit()

            dpg.set_value("status", f"Deleted record ID: {record_id}")

            # Reload data to update the display
            self.load_data()
            self.clear_form()
            cursor.close()
        except Exception as e:
            conn.rollback()
            dpg.set_value("status", f"Delete Error: {str(e)}")
            import traceback
            print(f"Delete Error details: {traceback.format_exc()}")
        finally:
            conn.close()


def main():
    manager = AcceptableGapsManager()

    # Initialize DearPyGUI
    dpg.create_context()
    dpg.create_viewport(title="Acceptable Gaps Manager", width=1200, height=800)

    # Register a font for better display
    with dpg.font_registry():
        default_font = None
        # Check if we're on Windows and try to use Segoe UI font
        if platform.system() == "Windows" and os.path.exists("C:/Windows/Fonts/segoeui.ttf"):
            default_font = dpg.add_font("C:/Windows/Fonts/segoeui.ttf", 16)

    # Apply font if available
    if default_font:
        dpg.bind_font(default_font)

    # Create a theme for invisible buttons
    with dpg.theme(tag="invisible_button_theme"):
        with dpg.theme_component(dpg.mvButton):
            dpg.add_theme_color(dpg.mvThemeCol_Button, [0, 0, 0, 0])
            dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, [0, 0, 0, 0])
            dpg.add_theme_color(dpg.mvThemeCol_ButtonActive, [0, 0, 0, 0])

    # Create window
    with dpg.window(label="Acceptable Gaps Manager", tag="main_window"):
        with dpg.group(horizontal=True):
            # Left side - Table display
            with dpg.child_window(width=800, height=400, tag="table_container"):
                # The gaps_table will be created in update_table_display
                pass

            # Right side - Form inputs
            with dpg.child_window(width=400, height=400):
                with dpg.group():
                    # Removed ID field as requested

                    dpg.add_text("Market:")
                    dpg.add_input_text(tag="market", hint="e.g., ES, NQ, CL", width=200)

                    # Start datetime
                    dpg.add_text("Start DateTime:")
                    with dpg.group(horizontal=True):
                        dpg.add_input_text(tag="start_date", width=120,
                                           default_value=datetime.now().strftime(DATE_FORMAT),
                                           hint="YYYY-MM-DD")
                        dpg.add_input_text(tag="start_time", width=120,
                                           default_value="00:00:00",
                                           hint="HH:MM:SS")

                    # End datetime
                    dpg.add_text("End DateTime:")
                    with dpg.group(horizontal=True):
                        dpg.add_input_text(tag="end_date", width=120,
                                           default_value=datetime.now().strftime(DATE_FORMAT),
                                           hint="YYYY-MM-DD")
                        dpg.add_input_text(tag="end_time", width=120,
                                           default_value="00:00:00",
                                           hint="HH:MM:SS")

                    dpg.add_text("Description:")
                    dpg.add_input_text(tag="description", multiline=True, width=380, height=60)

                    # Reoccurring settings using the enum
                    dpg.add_text("Reoccur Day of Week:")
                    # Set default to NotReoccurring
                    dpg.add_combo(items=manager.reoccur_day_options, default_value="NotReoccurring", tag="reoccur_day",
                                  width=200)

                    dpg.add_text("Reoccur Start Time:")
                    dpg.add_input_text(tag="reoccur_start_time", width=200,
                                       default_value="00:00:00",
                                       hint="HH:MM:SS")

                    dpg.add_text("Reoccur End Time:")
                    dpg.add_input_text(tag="reoccur_end_time", width=200,
                                       default_value="00:00:00",
                                       hint="HH:MM:SS")

        # Buttons
        with dpg.group(horizontal=True):
            dpg.add_button(label="Load Data", callback=manager.load_data)
            dpg.add_button(label="Add New", callback=manager.add_record)
            dpg.add_button(label="Update Selected", callback=manager.update_record)
            dpg.add_button(label="Delete Selected", callback=manager.delete_record)
            dpg.add_button(label="Clear Form", callback=manager.clear_form)

        # Status bar
        dpg.add_separator()
        dpg.add_text("Status: Ready", tag="status")

    # Initialize
    dpg.setup_dearpygui()
    dpg.show_viewport()
    dpg.set_primary_window("main_window", True)

    # Load data on startup
    manager.load_data()

    # Start the main loop
    dpg.start_dearpygui()
    dpg.destroy_context()


if __name__ == "__main__":
    main()