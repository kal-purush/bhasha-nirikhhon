import numpy as np
import datetime
import struct
import binascii

import ecg_scp as scp

LEADS_SCP = {
    0: scp.LEAD_I,
    1: scp.LEAD_II,
    2: scp.LEAD_III,
    3: scp.LEAD_AVR,
    4: scp.LEAD_AVL,
    5: scp.LEAD_AVF,
    6: scp.LEAD_V1,
    7: scp.LEAD_V2,
    8: scp.LEAD_V3,
    9: scp.LEAD_V4,
    10: scp.LEAD_V5,
    11: scp.LEAD_V6
}


class ECGBasic():

    def __init__(self):
        self.n_samples = 3600
        self.n_leads = 12
        self.data = np.zeros((self.n_leads, self.n_samples))
        # note: the leads order must be ['I', 'II', 'III', 'aVR', 'aVL', 'aVF', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6']

        self.sample_bits = 16  # (bit), long int in C, int in Python
        self.sample_rate = 360  # (Hz)
        self.ampl_nanovolt = 5000  # (nV), each 1 unit means 5000 nV
        # r peak is normally 0.5mV == 500uV == 100 * 5e3nV

        # timestamp of the ECG
        self.timestamp = datetime.datetime(year=2022, month=1, day=1, hour=1, minute=1, second=1)
        # patient information
        self.patient_name = "anonymized"
        self.patient_case = "unknown"
        self.patient_sex = 0  # female, or 1 for male
        self.patient_weight = 0
        self.patient_age = 0

    def export_scp(self, filename):
        """

        :param: filename: Str, path to store the .scp file
        """

        # Section pointers are required at least from #0 to #11.
        # s: sections to parse to SCP
        s = {}
        for sect_id in range(0, 12):
            s[sect_id] = b''

        # Prepare Section #0 - Section Pointers
        sect_id = 0
        length = scp.SECTION_HEADER_LEN + scp.POINTER_FIELD_LEN * 12
        index = scp.SCPECG_HEADER_LEN + 1
        s[0] = scp.make_pointer_field(sect_id, length, index)
        index += length
        for sect_id in range(1, 12):
            length = len(s[sect_id])
            if length > 0:
                length += scp.SECTION_HEADER_LEN
            s[0] += scp.make_pointer_field(sect_id, length, index)
            index += length

        # Prepare Section #1 - Patient Data
        # 1) Patient sex. (self.patient_sex)
        if self.patient_sex == 1:
            sex_code = scp.SEX_MALE
        elif self.patient_sex == 0:
            sex_code = scp.SEX_FEMALE
        else:
            sex_code = scp.SEX_UNKNOWN
        # 2) Patient weight.(self.patient_weight)
        weight_unit = scp.WEIGHT_UNSPECIFIED if self.patient_weight == 0 else scp.WEIGHT_KILOGRAM
        # 3) Patient age.(self.patient_age)
        age_unit = scp.AGE_UNSPECIFIED if self.patient_age == 0 else scp.AGE_YEARS
        # 4) Date and time of acquisition. (self.timestamp, self.patient_name, self.patient_vase)
        # t = datetime.datetime.strptime(self.timestamp, ECG90A_DATETIME_FORMAT)
        s[1] = scp.make_tag(scp.TAG_PATIENT_ID, scp.make_asciiz(self.patient_name))
        s[1] += scp.make_tag(scp.TAG_ECG_SEQ_NUM, scp.make_asciiz(self.patient_case))
        s[1] += scp.make_tag(scp.TAG_PATIENT_LAST_NAME, scp.make_asciiz(self.patient_name))
        s[1] += scp.make_tag(scp.TAG_PATIENT_SEX, struct.pack('<B', sex_code))
        s[1] += scp.make_tag(scp.TAG_PATIENT_WEIGHT, scp.make_3bytes_intval_unit(self.patient_weight, weight_unit))
        s[1] += scp.make_tag(scp.TAG_PATIENT_AGE, scp.make_3bytes_intval_unit(self.patient_age, age_unit))
        s[1] += scp.make_tag(scp.TAG_DATE_ACQ, scp.make_date(self.timestamp))
        s[1] += scp.make_tag(scp.TAG_TIME_ACQ, scp.make_time(self.timestamp))
        s[1] += scp.make_tag(scp.TAG_ACQ_DEV_ID, scp.make_machine_id('ECG90A'))
        s[1] += scp.make_tag(scp.TAG_EOF, b'')

        # Prepare Section #3 - ECG Lead Definition (self.samples)
        leads_number = self.n_leads
        flag_byte = 0b00000000
        flag_byte |= scp.ALL_SIMULTANEOUS_READ
        flag_byte |= (leads_number << 3)  # Simultaneous lead.
        s[3] = struct.pack('<B', leads_number)
        s[3] += struct.pack('<B', flag_byte)
        for i in range(0, leads_number):
            starting_sample = 1
            ending_sample = self.n_samples
            lead_id = LEADS_SCP[i]
            s[3] += struct.pack('<I', starting_sample)
            s[3] += struct.pack('<I', ending_sample)
            s[3] += struct.pack('<B', lead_id)

        # Prepare Section #6 - Rhythm data (self.sample_rate, self.samples)
        amplitude_multiplier = self.ampl_nanovolt
        sample_time_interval = int(1e6 / self.sample_rate)  # (us), In microseconds
        s[6] = struct.pack('<H', amplitude_multiplier)
        s[6] += struct.pack('<H', sample_time_interval)
        s[6] += struct.pack('<B', scp.ENCODING_REAL)
        s[6] += struct.pack('<B', scp.BIMODAL_COMPRESSION_FALSE)
        # Bytes to store for each serie, limited to 16bit size counter (sic!)
        max_samples = int(0xffff / (self.sample_bits / 8))
        bytes_to_store = int(min(self.n_samples, max_samples) * (self.sample_bits / 8))
        for i in range(0, leads_number):
            s[6] += struct.pack('<H', bytes_to_store)

        for i in range(self.n_leads):
            count = 0
            serie = b''
            for j in range(self.n_samples):
                val = self.data[i, j]
                # TODO: How to represent Null values in SCP-ECG?
                if val is None:
                    val = 0
                serie += struct.pack('<h', val)
                count += 1
                if count >= max_samples:
                    break
            s[6] += serie

        # Prepare SCP-ECG Record
        # CRC(2bytes) + Size(4bytes) + Section #0 + Section #1 + ...
        # s -> scp_ecg -> crc
        # scp_ecg = size + s
        size = scp.SCPECG_HEADER_LEN
        for sect_id in (0, 1, 2, 3, 6):
            if len(s[sect_id]) > 0:
                size += scp.SECTION_HEADER_LEN + len(s[sect_id])
        scp_ecg = struct.pack('<I', size)
        for sect_id in (0, 1, 2, 3, 6):
            if len(s[sect_id]) > 0:
                scp_ecg += scp.pack_section(sect_id, s[sect_id])
        crc = struct.pack('<H', binascii.crc_hqx(scp_ecg, 0xffff))

        with open(filename, "wb") as f:
            f.write(crc + scp_ecg)

        return filename