class NTADate(object):
    def __init__(self, day=1, month=1, year=2016):
        self.day = '%02d' % day
        self.month = '%02d' % month
        self.year = '%04d' % year

    def to_date(self):
        return '%s%s%s' % (self.year, self.month, self.day)


class NTATime(object):
    def __init__(self, hour, minute, second):
        self.hour = '%02d' % hour
        self.minute = '%02d' % minute
        self.second = '%02d' % second

    def to_time(self):
        return '%s%s%s' % (self.hour, self.minute, self.second)


class Event(object):
    _attr_tuple = tuple()

    def __init__(self, *args):
        self.__dict__ = dict(zip(self._attr_tuple, args))


class ChannelEnterEvent(Event):
    _attr_tuple = (
        'message_id', 'event_id', 'random_sequence', 'ca_card_number', 'sequence', 'enter_time', 'service_id', 'ts_id',
        'frequency',
        'channel_name', 'program_name', 'license', 'signal_intensity', 'signal_quality', 'is_sdv',
        'server_time', 'server_no')


class ChannelExitEvent(Event):
    _attr_tuple = (
        'message_id', 'event_id', 'random_sequence', 'ca_card_number', 'sequence', 'end_time', 'start_time',
        'service_id', 'ts_id',
        'frequency',
        'channel_name', 'program_name', 'license', 'signal_intensity', 'signal_quality', 'is_sdv', 'duration',
        'server_time', 'server_no')
