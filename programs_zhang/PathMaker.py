# -*-coding:utf-8-*-

import os

class NTADate:
    def __init__(self, nDay=1, nMonth=1, nYear=2016):
        self.Day = "%02d" % nDay
        self.Month = "%02d" % nMonth
        self.Year = str(nYear)
        self.Date = self.Year + self.Month + self.Day


class NTATime:
    def __init__(self, nHour, nMin, nSec):
        self.Hour = "%02d" % nHour
        self.Min = "%02d" % nMin
        self.Sec = "%02d" % nSec
        self.Time = self.Hour + self.Min + self.Sec


def make_in_path(ServerNo, Date, Time):
    BASE_DIR = os.path.dirname(__file__)
    server = "Server%02d" % ServerNo
    file_path  = os.path.join(BASE_DIR,"dataset",Date.Day,server + "_" + Date.Date + Time.Time + ".dat")
    #return "\\dataset\\" + Date.Day + "\\" + server + "_" + Date.Date + Time.Time + ".dat"
    return file_path

