import os
from datetime import date, datetime

from fpdf import FPDF
import matplotlib.pyplot as plt
import numpy as np
from pandas.plotting import register_matplotlib_converters
import pandas as pd


class Report:

    @staticmethod
    def printable_types():
        return {int, float, str, np.float64, date, datetime, pd.Timestamp}

    def __init__(self):
        self.row_width = 20
        self.row_height = 10
        self.font_family = 'Arial'
        self.font_size = 10
        

    def generate_report_dir(self):
        dir_path = os.path.join(os.path.expanduser('~'), 'backtester_database', 'report')
        if not os.path.isdir(dir_path):
            try:
                os.mkdir(dir_path)
            except:
                raise PermissionError('Unable to create reporting folder')
        return dir_path
    

    def generate_report(self, backtest_engine):
        """
        backtest_engine: core.engine.Engine object
        saves the PDF backtest report into report folder
        """
        report = FPDF()
        report.add_page()
        report.set_font(self.font_family, size=self.font_size)

        for field, value in vars(backtest_engine).items():
            if type(value) in Report.printable_types():
                report = self.print_row(report, field, value)
        
        report = self.print_list(report, backtest_engine.trades, 'Trades')
        report = self.generate_plot(report, backtest_engine.total_return_trend, 'Total Return Chart')
        folder_path = self.generate_report_dir()
        file_name = str(datetime.now()) + '.pdf'
        full_file_name = os.path.join(folder_path, file_name)
        report.output(full_file_name)


    def generate_plot(self, report, dataframe, plot_title):
        report.cell(self.row_width, self.row_height, txt=plot_title, ln=1)
        register_matplotlib_converters()
        dataframe.plot()
        folder_path = self.generate_report_dir()
        pic_path = os.path.join(folder_path, 'chart.png')
        plt.savefig(pic_path)
        report.image(pic_path, w=200)
        return report
    

    def print_list(self, report, item_list, section_header):
        report.cell(self.row_width, self.row_height, txt=section_header, ln=1)
        for item in item_list:
            if type(item) in Report.printable_types():
                report.cell(self.row_width, self.row_height, txt=str(item), ln=1)
            else:
                report = self.print_dict(report, vars(item))
        return report


    def print_dict(self, report, dictionary):
        report.cell(self.row_width, self.row_height, txt=str(dictionary), ln=1)
        return report


    def print_row(self, report, field, value):
        text = field + " : " + str(value)
        report.cell(self.row_width, self.row_height, txt=text, ln=1, align='L')
        return report


    def print_dataframe(self, report, dataframe):
        for col_name in dataframe.columns:
            report.cell(self.row_width, self.row_height, txt=col_name, border=1)
        
        report.ln(self.row_height)  
        for row_idx in range(dataframe.shape[0]):
            for col_idx in range(dataframe.shape[1]):
                report.cell(self.row_width, self.row_height, 
                    txt=str(dataframe.iloc[row_idx, col_idx], border=1))
            report.ln(self.row_width)
        return report 