import pymysql
import logging
import datetime
import os
from warnings import filterwarnings
from connect_to_db import connect_bd

EXECUTE_PATH = os.path.dirname(os.path.abspath(__file__))
LOG_D = 'log_organization'
LOG_DIR = f"{EXECUTE_PATH}/{LOG_D}"
TEMP_D = 'temp_organization'
TEMP_DIR = f"{EXECUTE_PATH}/{TEMP_D}"
if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)
# file_log = './log_organization/organization_ftp_' + str(datetime.date.today()) + '.log'
file_log = f'{LOG_DIR}/organization_ftp_{str(datetime.date.today())}.log'
filterwarnings('ignore', category=pymysql.Warning)
# file_log = './log_organization/organization_ftp_' + str(datetime.date.today()) + '.log'
logging.basicConfig(level=logging.DEBUG, filename=file_log,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
SUFFIX = '_from_ftp'
SUFFIXOD = ''
DB = 'tender'

if __name__ == "__main__":
    print('Привет, этот файл только для импортирования!')


def generator_univ(c):
    if type(c) == list:
        for i in c:
            yield i
    else:
        yield c


def get_el(d, *kwargs):
    res = ''
    l = len(kwargs)
    if l == 1:
        try:
            res = d[kwargs[0]]
        except Exception:
            res = ''
    elif l == 2:
        try:
            res = d[kwargs[0]][kwargs[1]]
        except Exception:
            res = ''
    elif l == 3:
        try:
            res = d[kwargs[0]][kwargs[1]][kwargs[2]]
        except Exception:
            res = ''
    elif l == 4:
        try:
            res = d[kwargs[0]][kwargs[1]][kwargs[2]][kwargs[3]]
        except Exception:
            res = ''
    elif l == 5:
        try:
            res = d[kwargs[0]][kwargs[1]][kwargs[2]][kwargs[3]][kwargs[4]]
        except Exception:
            res = ''
    elif l == 6:
        try:
            res = d[kwargs[0]][kwargs[1]][kwargs[2]][kwargs[3]][kwargs[4]][kwargs[5]]
        except Exception:
            res = ''
    if res is None:
        res = ''
    return res


def logging_parser(*kwargs):
    s_log = str(datetime.datetime.now()) + ' '
    for i in kwargs:
        s_log += f'{str(i)} '
    s_log += '\n\n'
    with open(file_log, 'a') as flog:
        flog.write(s_log)


class Organization:
    log_insert = 0
    log_update = 0
    regnumber_null = 0
    add_new_customer = 0
    update_new_customer = 0

    def __init__(self, org):
        self.org = org

    def get_org(self):
        if 'export' in self.org:
            if 'nsiOrganizationList' in self.org['export']:
                if 'nsiOrganization' in self.org['export']['nsiOrganizationList']:
                    orgs = generator_univ(self.org['export']['nsiOrganizationList']['nsiOrganization'])
                else:
                    return []
            else:
                return []
        else:
            return []
        return orgs

    @staticmethod
    def regNumber(og):
        return get_el(og, 'regNumber')

    @staticmethod
    def inn(og):
        return get_el(og, 'INN')

    @staticmethod
    def kpp(og):
        return get_el(og, 'KPP')

    @staticmethod
    def ogrn(og):
        return get_el(og, 'OGRN')

    @staticmethod
    def factual_address(og):
        return get_el(og, 'factualAddress', 'addressLine')

    @staticmethod
    def resp_role(og):
        try:
            return get_el(og, 'organizationRoles', 'organizationRoleItem', 'organizationRole')
        except Exception:
            return ''

    @staticmethod
    def region_code(og):
        r_c = get_el(og, 'factualAddress', 'region', 'kladrCode')
        if not r_c:
            r_c = get_el(og, 'factualAddress', 'city', 'kladrCode')
        if not r_c:
            r_c = get_el(og, 'factualAddress', 'area', 'kladrCode')
        if r_c:
            r_c = r_c[:2]
        return r_c

    @staticmethod
    def full_name(og):
        return get_el(og, 'fullName')

    @staticmethod
    def short_name(og):
        return get_el(og, 'shortName')

    @staticmethod
    def postal_address(og):
        return get_el(og, 'postalAddress')

    @staticmethod
    def phone(og):
        return get_el(og, 'phone')

    @staticmethod
    def fax(og):
        return get_el(og, 'fax')

    @staticmethod
    def email(og):
        return get_el(og, 'email')

    @staticmethod
    def actual(og):
        return get_el(og, 'actual')

    @staticmethod
    def register(og):
        return get_el(og, 'register')

    @staticmethod
    def contact_name(og):
        lastName = get_el(og, 'contactPerson', 'lastName')
        firstName = get_el(og, 'contactPerson', 'firstName')
        middleName = get_el(og, 'contactPerson', 'middleName')
        return '{0} {1} {2}'.format(firstName, middleName, lastName)


def parser_o(org, path):
    regNumber = Organization.regNumber(org)
    inn = Organization.inn(org)
    if not regNumber:
        Organization.regnumber_null += 1
        logging_parser('У организации нет regnum', path, inn)
        return
    kpp = Organization.kpp(org)
    ogrn = Organization.ogrn(org)
    region_code = Organization.region_code(org)
    full_name = Organization.full_name(org)
    short_name = Organization.short_name(org)
    postal_address = Organization.postal_address(org)
    factual_address = Organization.factual_address(org)
    resp_role = Organization.resp_role(org)
    phone = Organization.phone(org)
    fax = Organization.fax(org)
    email = Organization.email(org)
    contact_name = Organization.contact_name(org)
    actual = 1 if Organization.actual(org) == 'true' else 0
    register = 1 if Organization.register(org) == 'true' else 0
    contracts_count = 0
    contracts223_count = 0
    contracts_sum = 0.0
    contracts223_sum = 0.0
    con = connect_bd(DB)
    cur = con.cursor()
    cur.execute(f"""SELECT * FROM od_customer{SUFFIX} WHERE regNumber = %s """, (regNumber,))
    resultregnum = cur.fetchone()
    if not resultregnum:
        query1 = f"""INSERT INTO od_customer{SUFFIX} SET regNumber = %s, inn = %s, kpp = %s, contracts_count = %s, 
                contracts223_count = %s, contracts_sum = %s, contracts223_sum = %s, ogrn = %s, region_code = %s, 
                full_name = %s, short_name = %s, postal_address = %s, phone = %s, fax = %s, email = %s, 
                contact_name = %s, actual = %s, register = %s"""
        value1 = (
            regNumber, inn, kpp, contracts_count, contracts223_count, contracts_sum, contracts223_sum, ogrn,
            region_code,
            full_name, short_name, postal_address, phone, fax, email, contact_name, actual, register)
        cur.execute(query1, value1)
        Organization.log_insert += 1
        cur.execute(f"""SELECT id FROM od_customer{SUFFIXOD} WHERE regNumber=%s""", (regNumber,))
        res_c = cur.fetchone()
        if not res_c:
            query3 = f"""INSERT INTO od_customer{SUFFIXOD} SET regNumber = %s, inn = %s, kpp = %s, contracts_count = %s, 
                            contracts223_count = %s, contracts_sum = %s, contracts223_sum = %s, ogrn = %s, 
                            region_code = %s, 
                            full_name = %s, postal_address = %s, phone = %s, fax = %s, email = %s, 
                            contact_name = %s, short_name = %s"""
            value3 = (
                regNumber, inn, kpp, contracts_count, contracts223_count, contracts_sum, contracts223_sum, ogrn,
                region_code,
                full_name, postal_address, phone, fax, email, contact_name, short_name)
            cur.execute(query3, value3)
            Organization.add_new_customer += 1
    else:
        query2 = f"""UPDATE od_customer{SUFFIX} SET inn = %s, kpp = %s, contracts_count = %s, contracts223_count = %s, 
                contracts_sum = %s, contracts223_sum = %s, ogrn = %s, region_code = %s, full_name = %s, short_name = %s,
                postal_address = %s, phone = %s, fax = %s, email = %s, 
                contact_name = %s, actual = %s, register = %s WHERE regNumber = %s"""
        value2 = (
            inn, kpp, contracts_count, contracts223_count, contracts_sum, contracts223_sum, ogrn, region_code,
            full_name, short_name,
            postal_address, phone, fax, email, contact_name, actual, register, regNumber)
        cur.execute(query2, value2)
        Organization.log_update += 1
        cur.execute(f"""SELECT id FROM od_customer{SUFFIXOD} WHERE regNumber=%s""", (regNumber,))
        res_cc = cur.fetchone()
        if not res_cc:
            query4 = f"""INSERT INTO od_customer{SUFFIXOD} SET regNumber = %s, inn = %s, kpp = %s, contracts_count = %s, 
                                    contracts223_count = %s, contracts_sum = %s, contracts223_sum = %s, ogrn = %s, 
                                    region_code = %s, 
                                    full_name = %s, postal_address = %s, phone = %s, fax = %s, email = %s, 
                                    contact_name = %s, short_name = %s"""
            value4 = (
                regNumber, inn, kpp, contracts_count, contracts223_count, contracts_sum, contracts223_sum, ogrn,
                region_code,
                full_name, postal_address, phone, fax, email, contact_name, short_name)
            cur.execute(query4, value4)
            Organization.add_new_customer += 1
        # query4 = """UPDATE od_customer{0} SET inn = %s, kpp = %s, contracts_count = %s, contracts223_count = %s,
        #                 contracts_sum = %s, contracts223_sum = %s, ogrn = %s, region_code = %s, full_name = %s,
        #                 postal_address = %s, phone = %s, fax = %s, email = %s,
        #                 contact_name = %s WHERE regNumber = %s""".format(SUFFIX)
        # cur.execute(query4, value2)
        # Organization.update_new_customer += 1
    cur.execute(f"""SELECT * FROM organizer WHERE inn = %s  AND kpp = %s""", (inn, kpp))
    resinn = cur.fetchone()
    if not resinn:
        query4 = f"""INSERT INTO organizer SET reg_num = %s, full_name = %s, post_address = %s, fact_address = %s, inn = %s, kpp = %s, 
        responsible_role = %s, contact_person = %s, contact_email = %s, contact_phone = %s, contact_fax = %s"""
        value4 = (
            regNumber,full_name, postal_address, factual_address, inn, kpp, resp_role, contact_name, email, phone, fax)
        cur.execute(query4, value4)
    else:
        query4 = f"""UPDATE organizer SET reg_num = %s, full_name = %s, post_address = %s, fact_address = %s, 
                responsible_role = %s, contact_person = %s, contact_email = %s, contact_phone = %s, contact_fax = %s WHERE inn = %s AND kpp = %s"""
        value4 = (
            regNumber, full_name, postal_address, factual_address, resp_role, contact_name, email, phone, fax, inn, kpp)
        cur.execute(query4, value4)
    cur.close()
    con.close()


def parser(doc, path_xml):
    # global file_log
    o = Organization(doc)
    orgs = o.get_org()
    if not orgs:
        logging_parser('В файле нет списка организаций:', path_xml)
    for org in orgs:
        try:
            parser_o(org, path_xml)
        except Exception:
            logging.exception(f'Ошибка парсера {path_xml}')
