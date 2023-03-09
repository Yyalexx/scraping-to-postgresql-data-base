from time import sleep
from selenium import webdriver
import csv
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
import requests # Импортируем библиотеку requests для запросов
from bs4 import BeautifulSoup # Импортируем библиотеку BeautifulSoup для парсинга

# Общая часть адреса каталога рецептов
path = 'https://eda.ru/recepty?page='
# Юзер-агент
ua = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'

def grab_page_urls(url, ua):
    """
        Функция формирует адрес ссылки на рецепт.
            Args:
                url (string): адрес страницы каталога рецептов.
            Returns:
                url_list (list): список ссылок на рецепт.
    """
    # Выполняем GET-запрос, содержимое ответа присваивается переменной response
    response = requests.get(url, headers={'User-Agent': ua})
    # Создаём объект BeautifulSoup, указывая html-парсер
    page = str(BeautifulSoup(response.text, 'html.parser'))
    url_list = []
    # Начало блока с urls рецептов
    begin=page.find(
        '@type":"ItemList","itemListElement":[{"@type":"ListItem","url"')
    if begin > 0:
        # Конец блока с urls рецептов
        end=page.find('"@context":"http://schema.org"')
        if end > begin :
            # Неочищенный список urls рецептов
            url_list_r = page[begin:end].split('{"@type":"ListItem","url":"')
            # Очищенный список urls рецептов
            for i in range(1, len(url_list_r)):
                url_list.append({"url":url_list_r[i].split(',')[0][:-1]})
    return url_list

def write_to_file(url_list):
    """
        Функция построчно записывает ссылки в файл.
            Args:
                url_list (list): список ссылок на рецепт.
            Returns:
                None (none).
    """    
    for row in url_list:
        with open('./all_urls.csv', "a") as csvfile:
            fieldnames = ["url"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(row)

def run_proc(page_number, ua, path):
    """
        Функция построчно записывает ссылки в файл.
            Args:
                page_number (int): номер страницы каталога рецептов,
                ua (string): юзер-агент,
                path (string): общая часть адреса каталога рецептов.
            Returns:
                url_list (list): список ссылок на рецепт.
    """ 
    # Маскируем selenium
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(f'user-agent={ua}')
    browser = webdriver.Chrome(options=options)
    page_url = path+str(page_number)
    j = 0
    while j < 5: # Число попыток подключения
        browser.get(page_url)
        sleep(5)
        url_list = grab_page_urls(page_url, ua)
        j += 1
        if len(url_list) != 0:
            break
    write_to_file(url_list)
    return url_list
# Организуем многопоточность
# Учитывая ограничения оборудования парсим по 20 страниц в 10 потоков
i = 1
page_step = 20  # шаг перебора страниц
while True:
    futures = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        for number in range(i,i+page_step):
            futures.append(
                executor.submit(run_proc, number, ua, path)
            )
    wait(futures)
    urls_numb_list=[]
    for f in as_completed(futures):
            urls_numb_list.append(len(f.result()))
    print(f'Страницы {i}-{i+page_step}')
    print(urls_numb_list) #  Выводим список количества ссылок на страницах
    if 0 in urls_numb_list: # если ссылок на рецепты нет на одной из страниц
        break  # конец парсинга
    i += page_step
        
