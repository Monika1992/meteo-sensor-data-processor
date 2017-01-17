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


class Init(object):

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

        self.process()

    def process(self):
        config = Configuration()
        ftp = FTP_client()
        data_manager = Data_manager()
        logger = Logger()
        csv_manager = CSV_Manger()

        logger.clear_log_file(config.LOG_FILE_PATH)
        connection = ftp.connect_to_ftp(config.FTP_HOST, config.FTP_LOGIN, config.FTP_PSWD)
        start_date_object = data_manager.convert_string_date_to_date_object(self.start_date)
        end_date_object = data_manager.convert_string_date_to_date_object(self.end_date)
        dates = data_manager.get_dates_set(start_date_object, end_date_object)
        file_names = data_manager.get_file_names_from_dates(dates, config.FTP_FILE_EXTENSION)
        data_manager.create_dir_with_date_stamp(config.RUN_DATE, config.LOCAL_DIR)
        download_dir = os.path.join(config.LOCAL_DIR, config.RUN_DATE)

        # TODO: Split to methods!
        for file in file_names:
            csv_file_path = os.path.join(download_dir, file[:-4] + '.csv')

            ftp.download_data_from_ftp(connection, config.FTP_DIR_PATH, download_dir, file)
            file_content = data_manager.read_file_lines(os.path.join(download_dir, file))
            string_lines = re.split(r'\n', file_content)

            final_lines = []

            for line in string_lines:
                if line.startswith('PT'):
                    line_as_tuple = tuple(filter(None, re.split(r'\t+', line)))
                    necessary_values_of_line = data_manager.append_indexes_from_tuple_to_tuple(
                        config.INDEXES_OF_NEEDED_DATA, line_as_tuple)
                    final_lines.append(necessary_values_of_line)

            lines_with_correct_date_time_stamp = []
            for line in final_lines:
                separator_index = line[0].index('/')
                date_time_stamp = line[0][separator_index + 1:-9]
                clear_date_time = date_time_stamp.replace('T', ' ')
                list_line = list(line)
                list_line[0] = clear_date_time
                lines_with_correct_date_time_stamp.append(tuple(list_line))

            with open(csv_file_path, 'wb') as csv_f:
                file_writer = csv.writer(csv_f, delimiter=';')
                for x in lines_with_correct_date_time_stamp:
                    file_writer.writerow(x)

            ftp.disconnect_from_ftp(connection)
            csv_files = data_manager.list_files_with_given_extension(download_dir, config.CONNECTED_FILES_EXTENSION)

            csv_manager.connect_text_files_from_list_to_one(download_dir, csv_files, config.WEEK_FILE)
            csv_manager.split_csv_line_data_and_clear_lines(config.WEEK_FILE, config.WEEK_SUMMARY, config.HEADERS, ';', ',')


class FTP_client(object):

    def __init__(self):
        self.config_instance = Configuration()
        self.logger_instance = Logger()
        self.log_file_path = self.config_instance.LOG_FILE_PATH

    def connect_to_ftp(self, ftp_address, ftp_name, ftp_password):
        try:
            ftp_connection = FTP(ftp_address, ftp_name, ftp_password)
            self.logger_instance.add_log_message("Connected to FTP", self.log_file_path)
            return ftp_connection
        except ftplib.all_errors as e:
            self.logger_instance.add_log_message("Connecting to FTP failed with exception: " + str(e), self.log_file_path)
            return False

    def disconnect_from_ftp(self, ftp_connection):
        try:
            ftp_connection.quit()
            self.logger_instance.add_log_message("Disconnected from FTP" ,self.log_file_path)
        except ftplib.all_errors as e:
            self.logger_instance.add_log_message("Disconnecting from ftp failed with exception: " + str(e), self.log_file_path)
            return False

    def download_data_from_ftp(self, ftp_connection, ftp_address, local_address, file_name):
        try:
            ftp_connection.retrbinary('RETR %s' % ftp_address + '/' + file_name, open(os.path.join(local_address, file_name), 'wb').write)
            self.logger_instance.add_log_message("File: " + str(file_name) + " FTP download successful - local file path: " + str(os.path.join(local_address, file_name)), self.log_file_path)
        except ftplib.all_errors as e:
            self.logger_instance.add_log_message("Downloading " + str(file_name) + " from FTP failed with: " + str(e), self.log_file_path)
            return False

    def send_data_to_ftp(self, ftp_connection, ftp_path, local_path, file_name, ftp_address):
        try:
            ftp_connection.storbinary('RETR %s' % ftp_address + '/' + file_name, open(os.path.join(local_path,file_name), 'rb').read)
        except ftplib.all_errors as e:
            self.logger_instance.add_log_message("Uploading " + str(file_name) + "from FTP failed with: " + str(e), self.log_file_path)
            return False


class Data_manager(object):

    def __init__(self):
        self.config_instance = Configuration()
        self.logger_instance = Logger()
        self.log_file_path = self.config_instance.LOG_FILE_PATH

    def convert_string_date_to_date_object(self, string_date):
        return datetime.strptime(string_date, '%Y-%m-%d').date()

    def convert_number_if_negative(self, number):
        if number < 0:
            return int(number)*-1
        else:
            return number

    def get_dates_set(self, start_date_object, end_date_object):
        dates_list = []

        time_delta = end_date_object - start_date_object
        time_delta = self.convert_number_if_negative(time_delta.days)
        for i in range(0, time_delta + 1):
            date = start_date_object - timedelta(days=i)
            dates_list.append(str(date))

        return dates_list

    def get_file_names_from_dates(self, dates_list, file_extension):
        file_names = []

        for date in dates_list:
            file_names.append(date + file_extension)

        self.logger_instance.add_log_message("Downloading " + str(len(file_names)) + " files", self.log_file_path)
        self.logger_instance.add_log_message("File names: " + str(file_names), self.log_file_path)
        return file_names

    def create_dir_with_date_stamp(self, dir_name, basic_dir):
        try:
            os.chdir(basic_dir)
        except IOError as e:
            self.logger_instance.add_log_message("Failed to open dir " + str(basic_dir) + "with " + str(e), self.log_file_path)

        if not os.path.isdir(dir_name):
            try:
                os.mkdir(dir_name, 777)
            except IOError as e:
                self.logger_instance.add_log_message("Failed to create dir " + str(basic_dir + '/' + dir_name) + "with " + str(e), self.log_file_path)
                return False
        else:
            self.logger_instance.add_log_message("Directory " + str(dir_name) + " already exists! Overwriting data!", self.log_file_path)

    def read_file_lines(self, file_path):
        if os.path.exists(file_path):
            with open(file_path) as f:
                return f.read()
        else:
            self.logger_instance.add_log_message("Given file path does not exist " + str(file_path) + "! reading file content failed", self.log_file_path)
            return False

    def append_indexes_from_tuple_to_tuple(self, indexes_list, data_source_tuple):
        new_tuple = ()
        for index in indexes_list:
            new_tuple = new_tuple + (data_source_tuple[index],)
        return new_tuple

    # TODO: Exceptions handling
    def list_files_with_given_extension(self, dir_to_list, file_extension):
        searched_files = []

        if os.path.exists(dir_to_list):
            files = os.listdir(dir_to_list)
            for file in files:
                if file.endswith(file_extension):
                    searched_files.append(file)

        return searched_files


class CSV_Manger(object):

    def __init__(self):
        self.config_instance = Configuration()
        self.logger_instance = Logger()
        self.log_file_path = self.config_instance.LOG_FILE_PATH

    # TODO: Exceptions handling
    def connect_text_files_from_list_to_one(self, files_parent_dir, files_list, connected_file_path):
        os.chdir(files_parent_dir)
        with open(connected_file_path, 'ab') as file:
            for i in range(0, len(files_list)):
                for line in open(files_list[i]):
                    file.write(line)

    # TODO: Exceptions handling
    def split_csv_line_data_and_clear_lines(self, csv_file_to_edit, edited_file, csv_header_line_list, csv_delimiter, text_split_character):
        with open(csv_file_to_edit, 'r+') as in_f:
            with open(edited_file, 'ab') as out_f:
                out_file_writer = csv.writer(out_f, delimiter=str(csv_delimiter))
                out_file_writer.writerow(csv_header_line_list)
                for line in in_f:
                    out_f.write(str(csv_delimiter).join(line.split(str(text_split_character))).replace('[', '').replace(']', ''))

    def write_data_to_csv(self, csv_file_path, lines):
        with open(csv_file_path, 'wb') as resultFile:
            if os.path.exists(csv_file_path):
                wr = csv.writer(resultFile, dialect='excel', delimiter='/t')
                wr.writerows(lines)
            else:
                self.logger_instance.add_log_message("Given file path does not exist " + str(csv_file_path) + "! reading file content failed",self.log_file_path)


class Logger(object):

    def add_log_message(self, message_text, log_file_path):
        with open(log_file_path, 'ab') as f:
            f.writelines(self.create_message_text(message_text))
            f.close()

    def create_message_text(self, message_string):
        message_text = '[' + str(strftime("%Y-%m-%d %H:%M:%S", gmtime())) + ']: ' + message_string + os.linesep
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
    start_date = raw_input('Start of download (date as YYYY-MM-DD): ')
    end_date = raw_input('End of download (date as YYYY-MM-DD): ')

    Init(start_date, end_date)
