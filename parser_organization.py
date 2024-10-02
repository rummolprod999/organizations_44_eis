import ftplib
import pymysql
import logging
import datetime
import zipfile
import os
import shutil
import xmltodict
import parser_org
import timeout_decorator
from connect_to_db import connect_bd

# EXECUTE_PATH = os.path.dirname(os.path.abspath(__file__))
# LOG_D = 'log_organization'
# LOG_DIR = f"{EXECUTE_PATH}/{LOG_D}"
# TEMP_D = 'temp_prot'
# TEMP_DIR = f"{EXECUTE_PATH}/{TEMP_D}"
# if not os.path.exists(LOG_DIR):
#     os.mkdir(LOG_DIR)
# file_log = './log_organization/organization_ftp_' + str(datetime.date.today()) + '.log'
file_log = parser_org.file_log
temp_dir = parser_org.TEMP_DIR
logging.basicConfig(level=logging.DEBUG, filename=file_log,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
count_good = 0
count_bad = 0
except_file = ()
unic_files = []


def get_xml_to_dict(filexml, dirxml):
    """
    :param filexml: имя xml
    :param dirxml: путь к локальному файлу
    :return:
    """
    global count_good
    global count_bad

    path_xml = dirxml + '/' + filexml
    # print(path_xml)
    with open(path_xml) as fd:
        try:
            s = fd.read()
            s = s.replace("ns2:", "")
            s = s.replace("oos:", "")
            doc = xmltodict.parse(s)
            parser_org.parser(doc, path_xml)
            count_good += 1
            # with open(file_count_god, 'a') as good:
            #     good.write(str(count_good) + '\n')

        except Exception as ex:
            logging.exception("Ошибка: ")
            with open(file_log, 'a') as flog:
                flog.write(f'Ошибка конвертации в словарь {str(ex)} {path_xml}\n\n\n')
            count_bad += 1
            # with open(file_count_bad, 'a') as bad:
            #     bad.write(str(count_bad) + '\n')
            # return


def bolter(file, l_dir):
    """
    :param file: файл для проверки на черный список
    :param l_dir: директория локального файла
    :return: Если файл есть в черном списке - выходим
    """
    # print(f)
    try:
        get_xml_to_dict(file, l_dir)
    except Exception as exppars:
        logging.exception("Ошибка: ")
        with open(file_log, 'a') as flog:
            flog.write(f'Не удалось пропарсить файл {str(exppars)} {file}\n')


def get_list_ftp(path_parse):
    host = 'ftp.zakupki.gov.ru'
    ftpuser = 'free'
    password = 'otkluchenie_FTP_s_01_01_2025_podrobnee_v_ATFF'
    ftp2 = ftplib.FTP(host)
    ftp2.set_debuglevel(0)
    ftp2.encoding = 'utf8'
    ftp2.login(ftpuser, password)
    ftp2.cwd(path_parse)
    data = ftp2.nlst()
    array_ar = []
    for i in data:
        if i.find('2016') != -1 or i.find('2017') != -1 or i.find('2018') != -1 or i.find('2019') != -1 or i.find('2020') != -1 or i.find('2021') != -1 or i.find('2022') != -1 or i.find('2023') != -1 or i.find('2024') != -1:
            array_ar.append(i)

    return array_ar


def extract_prot(m, path_parse1):
    """
    :param m: имя архива с контрактами
    :param path_parse1: путь до папки с архивом
    """
    l = get_ar(m, path_parse1)
    if l:
        # print(l)
        r_ind = l.rindex('.')
        l_dir = l[:r_ind]
        os.mkdir(l_dir)
        try:
            z = zipfile.ZipFile(l, 'r')
            z.extractall(l_dir)
            z.close()
        except UnicodeDecodeError as ea:
            print('Не удалось извлечь архив ' + str(ea) + ' ' + l)
            with open(file_log, 'a') as floga:
                floga.write(f'Не удалось извлечь архив {str(ea)} {l}\n')
            try:
                os.system('unzip %s -d %s' % (l, l_dir))
            except Exception as ear:
                with open(file_log, 'a') as flogb:
                    flogb.write(f'Не удалось извлечь архив альтернативным методом {str(ear)} {l}\n')
                return
        except Exception as e:
            logging.exception("Ошибка: ")
            with open(file_log, 'a') as flogc:
                flogc.write(f'Не удалось извлечь архив {str(e)} {l}\n')
            return

        try:
            file_list = os.listdir(l_dir)
        except Exception as ex:
            logging.exception("Ошибка: ")
            with open(file_log, 'a') as flog:
                flog.write(f'Не удалось получить список файлов {str(ex)} {l_dir}\n')
        else:
            for f in file_list:
                bolter(f, l_dir)

        os.remove(l)
        shutil.rmtree(l_dir, ignore_errors=True)


@timeout_decorator.timeout(300)
def down_timeout(m, path_parse1):
    host = 'ftp.zakupki.gov.ru'
    ftpuser = 'free'
    password = 'otkluchenie_FTP_s_01_01_2025_podrobnee_v_ATFF'
    ftp2 = ftplib.FTP(host)
    ftp2.set_debuglevel(0)
    ftp2.encoding = 'utf8'
    ftp2.login(ftpuser, password)
    ftp2.cwd(path_parse1)
    local_f = '{0}/{1}'.format(temp_dir, str(m))
    lf = open(local_f, 'wb')
    ftp2.retrbinary('RETR ' + str(m), lf.write)
    lf.close()
    return local_f


def get_ar(m, path_parse1):
    """
    :param m: получаем имя архива
    :param path_parse1: получаем путь до архива
    :return: возвращаем локальный путь до архива или 0 в случае неудачи
    """
    retry = True
    count = 0
    while retry:
        try:
            lf = down_timeout(m, path_parse1)
            retry = False
            return lf
        except Exception as ex:
            logging.exception("Ошибка: ")
            with open(file_log, 'a') as flog:
                flog.write(f'Не удалось скачать архив {str(ex)} {m}\n')
            if count > 10:
                return 0
            count += 1


def main():
    with open(file_log, 'a') as flog:
        flog.write(f'Время начала работы парсера: {str(datetime.datetime.now())}\n')
    shutil.rmtree(temp_dir, ignore_errors=True)
    os.mkdir(temp_dir)
    path_parse = 'fcs_nsi/nsiOrganization/'
    try:
        # получаем список архивов
        arr_con = get_list_ftp(path_parse)
        for j in arr_con:
            try:
                extract_prot(j, path_parse)
            except Exception as exc:
                logging.exception("Ошибка: ")
                with open(file_log, 'a') as flog:
                    flog.write(f'Ошибка в экстракторе и парсере {str(exc)} {j}\n')
                continue

    except Exception as ex:
        logging.exception("Ошибка: ")
        with open(file_log, 'a') as flog:
            flog.write(f'Не удалось получить список архивов {str(ex)} {path_parse}\n')
    with open(file_log, 'a') as flog:
        flog.write(f'Добавлено заказчиков: {str(parser_org.Organization.log_insert)}\n')
        flog.write(f'Обновлено заказчиков: {str(parser_org.Organization.log_update)}\n')
        flog.write(f'Заказчиков без RegNumber: {str(parser_org.Organization.regnumber_null)}\n')
        flog.write(f'Добавлено в od_customer: {str(parser_org.Organization.add_new_customer)}\n')
        flog.write(f'Обновлено в od_customer: {str(parser_org.Organization.update_new_customer)}\n')
        flog.write(f'Время окончания работы парсера: {str(datetime.datetime.now())}\n\n\n')


if __name__ == "__main__":
    try:
        main()
    except Exception as exm:
        with open(file_log, 'a') as flogm:
            flogm.write(f'Ошибка в функции main: {str(exm)}\n\n\n')
