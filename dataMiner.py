#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ftplib import *
import ftplib
from time import gmtime, strftime
from datetime import datetime, timedelta
import os
import csv
import re
from config import Configuration


class Initializer(object):

    def check_input(self, date_string):
        datetime.strptime(str(date_string), '%Y-%m-%d')

    def process(self, start_date, end_date):
        config = Configuration()
        ftp = FTP_client()
        data_manager = Data_manager()
        logger = Logger()
        csv_manager = CSV_Manger(config, logger, data_manager)

        logger.clear_log_file(config.LOG_FILE_PATH)
        logger.add_log_message("Data processing started", config.LOG_FILE_PATH, config.LOGGER_START)
        connection = ftp.connect_to_ftp(config.FTP_HOST, config.FTP_LOGIN, config.FTP_PSWD)
        start_date_object = data_manager.convert_string_date_to_date_object(start_date)
        end_date_object = data_manager.convert_string_date_to_date_object(end_date)
        dates = data_manager.get_dates_set(start_date_object, end_date_object)
        weeks_list =  data_manager.cut_dates_set_to_weeks(dates)

        data_manager.create_dir_with_date_stamp(config.RUN_DATE, config.LOCAL_DIR)
        download_dir = os.path.join(config.LOCAL_DIR, config.RUN_DATE)

        for week_dates_list in weeks_list:
            file_names = data_manager.get_file_names_from_dates(week_dates_list, config.FTP_FILE_EXTENSION)
            data_manager.create_dir_with_date_stamp(week_dates_list[0], download_dir)
            week_download_dir = os.path.join(download_dir, week_dates_list[0])

            for file in file_names:
                logger.add_log_message("Processing file: " + str(file), config.LOG_FILE_PATH, config.LOGGER_INFO)
                csv_file_path = os.path.join(week_download_dir, file[:-4] + '.csv')

                logger.add_log_message("Downloading data from FTP", config.LOG_FILE_PATH, config.LOGGER_INFO)
                ftp.download_data_from_ftp(connection, config.FTP_DIR_PATH, week_download_dir, file)

                logger.add_log_message("Clearing data", config.LOG_FILE_PATH, config.LOGGER_INFO)
                file_content = data_manager.read_file_lines(os.path.join(week_download_dir, file))
                string_lines = re.split(r'\n', file_content)
                pure_data_lines = csv_manager.get_pure_data_lines_from_csv(string_lines, config.DATA_LINES_POINTER)
                lines_with_correct_date_time_stamp = data_manager.adjust_date_time_stamp_in_lines(pure_data_lines, '/', 'T', ' ')

                logger.add_log_message("Converting file: " + str(file) + " to CSV format", config.LOG_FILE_PATH, config.LOGGER_INFO)
                csv_manager.write_data_to_csv(csv_file_path, lines_with_correct_date_time_stamp, config.CSV_DELIMITER)

            logger.add_log_message("Connecting week CSV files", config.LOG_FILE_PATH, config.LOGGER_INFO)
            csv_files = data_manager.list_files_with_given_extension(week_download_dir, config.CONNECTED_FILES_EXTENSION)
            this_week_csv_file_name = data_manager.create_file_name_from_dates(week_dates_list[0], week_dates_list[-1], '',config.CONNECTED_FILES_EXTENSION)
            this_week_csv_summary_file_name = data_manager.create_file_name_from_dates(week_dates_list[0], week_dates_list[-1], '_week_sum',config.CONNECTED_FILES_EXTENSION)
            csv_manager.connect_text_files_from_list_to_one(week_download_dir, csv_files, this_week_csv_file_name)
            csv_manager.split_csv_line_data_and_clear_lines(this_week_csv_file_name, this_week_csv_summary_file_name, config.HEADERS, str(config.CSV_DELIMITER), ',')
            logger.add_log_message("Files processing finalized", config.LOG_FILE_PATH, config.LOGGER_INFO)

        ftp.disconnect_from_ftp(connection)
        logger.add_log_message("Data processing end", config.LOG_FILE_PATH, config.LOGGER_END)

class FTP_client(object):

    def __init__(self):
        self.config_instance = Configuration()
        self.logger_instance = Logger()
        self.log_file_path = self.config_instance.LOG_FILE_PATH

    def connect_to_ftp(self, ftp_address, ftp_name, ftp_password):
        try:
            ftp_connection = FTP(ftp_address, ftp_name, ftp_password)
            self.logger_instance.add_log_message("Connected to FTP", self.log_file_path, self.config_instance.LOGGER_INFO)
            return ftp_connection
        except ftplib.all_errors as e:
            self.logger_instance.add_log_message("Connecting to FTP failed with exception: " + str(e), self.log_file_path, self.config_instance.LOGGER_ERROR)
            return False

    def disconnect_from_ftp(self, ftp_connection):
        try:
            ftp_connection.quit()
            self.logger_instance.add_log_message("Disconnected from FTP" ,self.log_file_path, self.config_instance.LOGGER_INFO)
        except ftplib.all_errors as e:
            self.logger_instance.add_log_message("Disconnecting from ftp failed with exception: " + str(e), self.log_file_path, self.config_instance.LOGGER_ERROR)
            return False

    def download_data_from_ftp(self, ftp_connection, ftp_address, local_address, file_name):
        try:
            ftp_connection.retrbinary('RETR %s' % ftp_address + '/' + file_name, open(os.path.join(local_address, file_name), 'wb').write)
            self.logger_instance.add_log_message("File: " + str(file_name) + " FTP download successful - local file path: " + str(os.path.join(local_address, file_name)), self.log_file_path, self.config_instance.LOGGER_INFO)
        except ftplib.all_errors as e:
            self.logger_instance.add_log_message("Downloading " + str(file_name) + " from FTP failed with: " + str(e), self.log_file_path, self.config_instance.LOGGER_ERROR)
            return False

    def send_data_to_ftp(self, ftp_connection, ftp_path, local_path, file_name, ftp_address):
        try:
            ftp_connection.storbinary('RETR %s' % ftp_address + '/' + file_name, open(os.path.join(local_path,file_name), 'rb').read)
        except ftplib.all_errors as e:
            self.logger_instance.add_log_message("Uploading " + str(file_name) + " from FTP failed with: " + str(e), self.log_file_path, self.config_instance.LOGGER_ERROR)
            return False


class Data_manager(object):

    def __init__(self):
        self.config_instance = Configuration()
        self.logger_instance = Logger()
        self.log_file_path = self.config_instance.LOG_FILE_PATH

    def create_file_name_from_dates(self,start_date, end_date, flag_text, file_extension):
        return str(start_date) + '_' + str(end_date) + str(flag_text) + str(file_extension)

    def adjust_date_time_stamp_in_lines(self, lines_list, hash_delimiter, date_time_delimiter_to_replace, replace_char):
        corrected_lines = []
        for line in lines_list:
            separator_index = line[0].index(str(hash_delimiter))
            date_time_stamp = line[0][separator_index + 1:-9]
            clear_date_time = date_time_stamp.replace(str(date_time_delimiter_to_replace), str(replace_char))
            list_line = list(line)
            list_line[0] = clear_date_time
            corrected_lines.append(tuple(list_line))

        return corrected_lines

    def cut_dates_set_to_weeks(self, dates_list):
        if dates_list:
            week_dates_set = [dates_list[x:x+8] for x in xrange(0, len(dates_list), 8)]
            return week_dates_set
        else:
            self.logger_instance.add_log_message("Empty dates list given as attribute in Data_manager.cut_dates_set_to_weeks! Data preparation failed!", self.log_file_path, self.config_instance.LOGGER_ERROR)

    def convert_string_date_to_date_object(self, string_date):
        return datetime.strptime(string_date, '%Y-%m-%d').date()

    def __convert_number_if_negative(self, number):
        if number < 0:
            return int(number)*-1
        else:
            return number

    def get_dates_set(self, start_date_object, end_date_object):
        dates_list = []

        time_delta = end_date_object - start_date_object
        time_delta = self.__convert_number_if_negative(time_delta.days)
        for i in range(0, time_delta + 1):
            date = start_date_object - timedelta(days=i)
            dates_list.append(str(date))

        return dates_list

    def get_file_names_from_dates(self, dates_list, file_extension):
        file_names = []

        for date in dates_list:
            file_names.append(date + file_extension)

        self.logger_instance.add_log_message("Downloading " + str(len(file_names)) + " files", self.log_file_path, self.config_instance.LOGGER_INFO)
        self.logger_instance.add_log_message("File names: " + str(file_names), self.log_file_path, self.config_instance.LOGGER_INFO)
        return file_names

    def create_dir_with_date_stamp(self, dir_name, basic_dir):
        try:
            os.chdir(basic_dir)
        except IOError as e:
            self.logger_instance.add_log_message("Failed to open dir " + str(basic_dir) + " with " + str(e), self.log_file_path, self.config_instance.LOGGER_ERROR)

        if not os.path.isdir(dir_name):
            try:
                os.mkdir(dir_name, 777)
            except IOError as e:
                self.logger_instance.add_log_message("Failed to create dir " + str(basic_dir + '/' + dir_name) + " with " + str(e), self.log_file_path, self.config_instance.LOGGER_ERROR)
                return False
        else:
            self.logger_instance.add_log_message("Directory " + str(dir_name) + " already exists! Overwriting data!", self.log_file_path, self.config_instance.LOGGER_WARNING)

    def read_file_lines(self, file_path):
        if os.path.exists(file_path):
            with open(file_path) as f:
                return f.read()
        else:
            self.logger_instance.add_log_message("Given file path does not exist " + str(file_path) + "! Reading file content failed", self.log_file_path, self.config_instance.LOGGER_ERROR)
            return False

    def append_indexes_from_tuple_to_tuple(self, indexes_list, data_source_tuple):
        new_tuple = ()
        for index in indexes_list:
            new_tuple = new_tuple + (data_source_tuple[index],)
        return new_tuple

    def list_files_with_given_extension(self, dir_to_list, file_extension):
        searched_files = []
        if os.path.exists(dir_to_list):
            files = os.listdir(dir_to_list)
            for file in files:
                if file.endswith(file_extension):
                    searched_files.append(file)
            return searched_files
        else:
            self.logger_instance.add_log_message("Given dir path does not exist " + str(dir_to_list) + "! Listing files failed", self.log_file_path, self.config_instance.LOGGER_ERROR)


class CSV_Manger(object):

    def __init__(self, configuration_instance, logger_instance, data_manager_instance):
        self.config_instance = configuration_instance
        self.logger_instance = logger_instance
        self.data_manager = data_manager_instance
        self.log_file_path = self.config_instance.LOG_FILE_PATH

    def connect_text_files_from_list_to_one(self, files_parent_dir, files_list, connected_file_path):
        if files_list:
            if os.path.isdir(files_parent_dir):
                os.chdir(files_parent_dir)
                with open(connected_file_path, 'ab') as file:
                    if os.path.exists(connected_file_path):
                        for i in range(0, len(files_list)):
                            for line in open(files_list[i]):
                                file.write(line)
                    else:
                        self.logger_instance.add_log_message( "Given dir path does not exist " + str(connected_file_path) + "! Connecting CSV files failed", self.log_file_path, self.config_instance.LOGGER_ERROR)
            else:
                self.logger_instance.add_log_message("Given dir path does not exist " + str(files_parent_dir) + "! Connecting CSV files failed",self.log_file_path, self.config_instance.LOGGER_ERROR)
        else:
            self.logger_instance.add_log_message("Empty files list given as attribute in CSV_Manager.connect_text_files_from_list_to_one! Connecting CSV files failed", self.log_file_path, self.config_instance.LOGGER_ERROR)

    def split_csv_line_data_and_clear_lines(self, csv_file_to_edit, edited_file, csv_header_line_list, csv_delimiter, text_split_character):
        with open(csv_file_to_edit, 'r+') as in_f:
            if os.path.exists(csv_file_to_edit):
                with open(edited_file, 'ab') as out_f:
                    if os.path.exists(edited_file):
                        out_file_writer = csv.writer(out_f, delimiter=str(csv_delimiter))
                        out_file_writer.writerow(csv_header_line_list)
                        for line in in_f:
                            out_f.write(str(csv_delimiter).join(line.split(str(text_split_character))).replace('[', '').replace(']', ''))
                    else:
                        self.logger_instance.add_log_message(
                            "Given file path does not exist " + str(csv_file_to_edit) + "! Reading file content failed", self.log_file_path, self.config_instance.LOGGER_ERROR)
            else:
                self.logger_instance.add_log_message("Given file path does not exist " + str(csv_file_to_edit) + "! Reading file content failed", self.log_file_path, self.config_instance.LOGGER_ERROR)

    def write_data_to_csv(self, csv_file_path, lines, delimiter):
        with open(csv_file_path, 'wb') as resultFile:
            if os.path.exists(csv_file_path):
                writer = csv.writer(resultFile, delimiter=delimiter)
                for row in lines:
                    writer.writerow(row)
            else:
                self.logger_instance.add_log_message("Given file path does not exist " + str(csv_file_path) + "! Reading file content failed", self.log_file_path, self.config_instance.LOGGER_ERROR)

    def get_pure_data_lines_from_csv(self, csv_file_lines_list, data_lines_pointer_text):
        if csv_file_lines_list:
            pure_data_lines = []
            for line in csv_file_lines_list:
                if line.startswith(str(data_lines_pointer_text)):
                    line_as_tuple = tuple(filter(None, re.split(r'\t+', line)))
                    necessary_values_of_line = self.data_manager.append_indexes_from_tuple_to_tuple(self.config_instance.INDEXES_OF_NEEDED_DATA, line_as_tuple)
                    pure_data_lines.append(necessary_values_of_line)

            return pure_data_lines
        else:
            self.logger_instance.add_log_message("Missing input data in CSV_Manager.get_pure_lines_from_csv - clearing data failed", self.log_file_path, self.config_instance.LOGGER_ERROR)

class Logger(object):

    def add_log_message(self, message_text, log_file_path, log_type):
        with open(log_file_path, 'ab') as f:
            f.writelines(self.create_message_text(message_text, log_type))
            f.close()

    def create_message_text(self, message_string, log_type):
        message_text = '[' + str(strftime("%Y-%m-%d %H:%M:%S", gmtime())) + ']:['+ str(log_type).upper() +']:   ' + message_string + os.linesep
        return message_text

    def clear_log_file(self, log_file):
        with open(log_file, 'wb'):
            pass

    def check_if_log_file_is_not_empty(self, log_file):
        if os.path.exists(log_file) and os.path.getsize(log_file) > 0:
            return True
        else:
            return False


if __name__ == '__main__':

    while True:
        start_date = raw_input('Start of download (date as YYYY-MM-DD): ')
        end_date = raw_input('End of download - in the past(date as YYYY-MM-DD): ')
        try:
            Initializer().check_input(start_date)
            Initializer().check_input(end_date)
        except ValueError:
            print 'Bad date time format for one of given dates, please check spelling and repeat input'
            continue
        else:
            print 'Running processing for dates from: ' + str(start_date) + ' to: ' + str(end_date) + '! Process description can be found at given logger file'
            Initializer().process(start_date, end_date)
            print 'Process finalized'
            exit()




