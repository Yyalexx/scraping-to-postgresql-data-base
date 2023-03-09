import requests # Импортируем библиотеку requests для запросов
from bs4 import BeautifulSoup # Импортируем библиотеку BeautifulSoup для парсинга
import bleach  # Импортируем библиотеку bleach для очистки текста от тэгов
import pandas as pd
import os.path
import csv
from time import sleep
from selenium import webdriver
import csv
from concurrent.futures import ThreadPoolExecutor, wait, as_completed

columns_list=['recipe_id', 'cuisine', 'meal_mode', 'recipe_name', 
            'portions', 'calories', 'proteinContent',
            'fatContent', 'carbohydrateContent', 'ingr_name', 
            'ingr_qwn', 'measure', 'quantity', 'text']

url_df = pd.read_csv('all_urls.csv', header=None, names=['url'], skiprows=[0])
ua = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'

def grab_recipe(url, ua):
    """
        Функция формирует словарь рецепта.
            Args:
                url (string): адрес страницы рецепта,
                ua (string): user agent.
            Returns:
                recipe (dict): словарь рецепта.
    """
    try:
        # Выполняем GET-запрос, содержимое ответа присваивается переменной response
        response = requests.get(url, headers={'User-Agent': ua})
        # Создаём объект BeautifulSoup, указывая html-парсер
        page = BeautifulSoup(response.text, 'html.parser') 
    except:
        pass
    # Кухня
    try:
        cuisine = page.title.text.split(" – ")[1].split(":")[0]
    except:
        cuisine = None
    # meal_mode    
    try:
        meal_mode = page.title.text.split(": ")[1].split(".")[0]
    except:
        meal_mode = None
    # Название рецепта
    try:
        recipe_name = page.find('h1').text.replace('\xa0',' ')
    except:
        recipe_name = None
    # portions
    try:
        portions = page.find(itemprop="recipeYield")
        portions = int(bleach.clean(str(portions), tags=[], strip=True))
    except:
        portions = None
    # calories
    try:
        calories = page.find(itemprop="calories")
        calories= int(bleach.clean(str(calories), tags=[], strip=True))
    except:
        calories = None
    # proteinContent
    try:
        proteinContent = page.find(itemprop="proteinContent")
        proteinContent= int(bleach.clean(str(proteinContent), tags=[], strip=True))
    except:
        proteinContent = None
    # fatContent
    try:
        fatContent = page.find(itemprop="fatContent")
        fatContent= int(bleach.clean(str(fatContent), tags=[], strip=True))
    except:
        fatContent = None
    # carbohydrateContent
    try:
        carbohydrateContent = page.find(itemprop="carbohydrateContent")
        carbohydrateContent= int(bleach.clean(str(carbohydrateContent), tags=[], strip=True))
    except:
        carbohydrateContent = None
    # ingr_names_list
    ingr_names = page.find_all(itemprop="recipeIngredient")
    
    ingr_names_list = []
    for ingr_name in ingr_names:
        ingr_name =  bleach.clean(str(ingr_name), tags=[], strip=True)
        ingr_names_list.append(ingr_name)
    # ingr_qwn_list
    ingr_qwn_list = []
    for ingr in ingr_names_list:
        begin = str(page).find('recipeIngredient">'+str(ingr))+18
        end = begin+200
        interval = str(page)[begin:end]
        len_ingr = len(ingr)
        # lstrip тут работает не так
        ingr_qwn =(bleach.clean(interval, tags=[], strip=True))[len_ingr:].split('\n')[0]
        ingr_qwn_list.append(ingr_qwn)

    # Текст рецепта
    rtl = page.find_all(itemprop="recipeInstructions")
    recipe_text_list = []
    for i in range(len(rtl)):
        recipe_step = rtl[i]
        recipe_step = bleach.clean(str(recipe_step), tags=[], strip=True)
        recipe_step = recipe_step.replace('\n','').replace('\xa0',' ')
        if i>=9:
            a = 2
        else: a = 1
        recipe_step = ("").join(recipe_step.split("Шпаргалка")[0])
        recipe_step = ("").join(recipe_step.split("Инструмент")[0])[a:]
        recipe_text_list.append(recipe_step)
    recipe_text = (" ").join(recipe_text_list)    

    # Словарь текущего рецепта
    recipe = {}
    recipe['ingr_name'] = ingr_names_list
    recipe['ingr_qty_unit'] = ingr_qwn_list
    recipe['name_recipe'] = recipe_name
    recipe['portions'] = portions
    recipe['calories'] = calories
    recipe['protein'] = proteinContent
    recipe['fat'] = fatContent
    recipe['carbo'] = carbohydrateContent
    recipe['text_recipe'] = recipe_text
    recipe['cuisine'] = cuisine
    recipe['meal_mode'] = meal_mode

    return recipe

def write_to_file(recipe):
    """
        Функция построчно записывает рецепты в файл.
            Args:
                recipe (dict): словарь рецепта.
            Returns:
                None (none).
    """ 
    fieldnames = ['cuisine',
                'meal_mode',
                'name_recipe',
                'portions',
                'calories',
                'protein',
                'fat',
                'carbo',
                'ingr_name',
                'ingr_qty_unit',
                'text_recipe'
                ]
    recipe_list=[]
    for i in range (len(recipe['ingr_name'])):
        curr_dict={}
        curr_dict['ingr_name'] = recipe['ingr_name'][i]
        curr_dict['ingr_qty_unit'] = recipe['ingr_qty_unit'][i]
        curr_dict['name_recipe'] = recipe['name_recipe']
        curr_dict['portions'] = recipe['portions']
        curr_dict['calories'] = recipe['calories']
        curr_dict['protein'] = recipe['protein']
        curr_dict['fat'] = recipe['fat']
        curr_dict['carbo'] = recipe['carbo']
        curr_dict['text_recipe'] = recipe['text_recipe']
        curr_dict['cuisine'] = recipe['cuisine']
        curr_dict['meal_mode'] = recipe['meal_mode'] 
        recipe_list.append(curr_dict)

    for row in recipe_list:
        with open('./recipes.csv', "a") as csvfile:
            fieldnames = fieldnames
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(row)


def run_proc(url, ua):
    """
        Функция построчно записывает рецепты в файл.
            Args:
                url (string): адрес страницы рецепта,
                ua (string): user agent.
            Returns:
                recipe (dict): словарь рецепта.
    """ 
    # Маскируем selenium
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(f'user-agent={ua}')
    browser = webdriver.Chrome(options=options)
    
    j = 0
    while j < 30: # Число попыток подключения
        browser.get(url)
        sleep(5)
        recipe = grab_recipe(url, ua)
        j += 1
        if len(recipe['ingr_name']) != 0:
            break
    write_to_file(recipe)
    return recipe

# Организуем многопоточность
# Учитывая ограничения оборудования парсим по 20 страниц в 10 потоков
i = 1
page_step = 20  # шаг перебора страниц
stop = len(url_df)+1

while i < stop:
    futures = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        for number in range(i,i+page_step):
            url = url_df.url[number]
            futures.append(
                executor.submit(run_proc, url, ua)
            )
    wait(futures)
    rec_numb_list=[]
    for f in as_completed(futures):
            rec_numb_list.append(len(f.result()['ingr_name']))
    print(f'Страницы {i}-{i+page_step-1}')
    print(rec_numb_list) #  Выводим список количества ингредиентов в рецептах

    i += page_step
