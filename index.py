import requests
import json
from bs4 import BeautifulSoup
import lxml
import re
from pprint import pprint
import csv
import os
import sys

from multiprocessing import Pool
import multiprocessing
from urllib.parse import quote

base_url = 'https://www.zomato.com/auckland'
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}
google_map_url = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json?&inputtype=textquery&key=AIzaSyBfWjC_oswm-c-JDl_gGZ4f2wKBsxLe_O4&fields=rating,user_ratings_total'

# Shape of result for a restaurant file for all regions
# result: {
#   num_places: 700,
#   restaurants: [
#     { location(lat, lng), phone number, name, cuisine },
#   ]

class Scrape:
  def __init__(self):
    self.result = {}

  def get_restaurants_in_page(self, soup):
    restaurants = soup.select('div.card.search-snippet-card.search-card div.search_left_featured.clearfix a')
    return list(map(lambda r: r['href'], restaurants))

  def get_restaurant_details(self, link, region_name):
    r = requests.get(link, headers=headers)
    restaurant_soup = make_soup(r.text)

    lat_tag = restaurant_soup.find('meta', property='place:location:latitude')
    lat = lat_tag['content']
    lon_tag = restaurant_soup.find('meta', property='place:location:longitude')
    lon = lon_tag['content']

    name = restaurant_soup.select('h1.res-name.left.mb0 a')[0].get_text().strip()
    cuisine_tags = restaurant_soup.select('div.res-info-cuisines.clearfix a')
    cuisine = ', '.join(list(map(lambda c: c.get_text().strip(), cuisine_tags)))

    phone_tag = restaurant_soup.select('div#phoneNoString span span span')
    phone_number = ', '.join(list(map(lambda p: p.get_text().strip(), phone_tag)))

    restaurant = {
      'name': name,
      'locality': region_name,
      'latitude': lat,
      'longitude': lon,
      'cuisine': cuisine,
      'phone number': phone_number,
      'google rating': '',
      'number of ratings': ''
    }

    search_term = '{} {}'.format(name, region_name)
    location_bias = '{},{}'.format(lat, lon)

    google_response = requests.get(google_map_url + '&input=' + quote(search_term) + '&locationbias=' + location_bias)
    json_response = google_response.json()
    if json_response.get('status', None) == 'OK' and json_response.get('candidates', None) is not None:
      if len(json_response['candidates']) == 1:
        candidate = json_response['candidates'][0]

        if candidate:
          restaurant['google rating'] = candidate['rating']
          restaurant['number of ratings'] = candidate['user_ratings_total']
        else:
          restaurant['google rating'] = 'no rating'
          restaurant['number of ratings'] = 'no rating'
      elif len(json_response['candidates']) > 1:
        restaurant['google rating'] = 'multiple ratings'
        restaurant['number of ratings'] = 'multiple ratings'
      elif len(json_response['candidates']) == 0:
        restaurant['google rating'] = 'restaurant not found'
        restaurant['number of ratings'] = 'restaurant not found'
    else:
      restaurant['google rating'] = 'encountered error'
      restaurant['number of ratings'] = 'encountered error'
    
    return restaurant


  def get_restaurants_in_region(self, region):
    region_link = region['link']
    region_name = region['region']
    # num_restaurants = 0
    restaurants = []

    print('Getting {}\'s url: {}'.format(region_name, region_link))
    r = requests.get(region_link, headers=headers, params={ 'all': 1, 'nearby': 0 })
    region_soup = make_soup(r.text)

    page_soup = region_soup.select('div.col-l-4.mtop.pagination-number div')[0]
    page_number_regex = r'of (\d+)'
    match = re.search(page_number_regex, page_soup.get_text())

    if match is not None:
      num_pages = int(match.group(1))
      print('There are {} pages for region {}'.format(num_pages, region_name))

      restaurants_in_page = self.get_restaurants_in_page(region_soup)

      # page 1
      for r in restaurants_in_page:
        restaurant_detail = self.get_restaurant_details(r, region_name)
        restaurants.append(restaurant_detail)
      
      # all other pages
      for curr_page in range(2, num_pages + 1):
        print('Getting page {} out of {} for {}'.format(curr_page, num_pages, region_name))
        r = requests.get(region_link, headers=headers, params={ 'page': curr_page })
        restaurants_in_page = self.get_restaurants_in_page(make_soup(r.text))

        for r in restaurants_in_page:
          restaurant_detail = self.get_restaurant_details(r, region_name)
          restaurants.append(restaurant_detail)
      
      self.result['restaurants'] = restaurants

  def save_to_csv(self):
    for region_name in self.result.keys():
      with open('{}.csv'.format(region_name), 'w', newline='') as csvfile:
        fieldnames = ['name', 'locality', 'phone number', 'cuisine', 'latitude', 'longitude',]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        restaurants = self.result[region_name]['restaurants']
        for r in restaurants:
          writer.writerow(r)

def prettyPrint(jsonObj):
  print(json.dumps(jsonObj, indent=4))

def make_soup(content):
  return BeautifulSoup(content, 'lxml')

# BS helpers
def get_region_name_places(region):
  stripped = re.sub(r'\s+', ' ', region.get_text().strip())
  
  region_places_regex = r'(\w+\s?\w+)\s\((\d+)'
  match = re.search(region_places_regex, stripped)

  if match is not None:
    region_name = match.group(1)
    num_places = match.group(2)
    return [region_name, num_places]

def get_regions(soup):
  res = []
  regions = soup.select('h2.ui.header + div.ui.segment.row a')

  for region in regions:
    link = region['href']
    [region_name, num_places] = get_region_name_places(region)
    res.append({ 'link': link, 'region': region_name, 'num_places': num_places })
  
  return res

def listener(q):
    '''listens for messages on the q, writes to file. '''
    with open('zomato-auckland.csv', 'w', newline='') as csvfile:
      fieldnames = ['name', 'locality', 'phone number', 'cuisine', 'latitude', 'longitude', 'google rating', 'number of ratings']
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
      writer.writeheader()
      
      while 1:
        rows = q.get()
        if rows == 'kill':
          break
        for row in rows:
          writer.writerow(row)
        

def main(region, q):
  scrape = Scrape()
  region_name = region['region']
  scrape.result['restaurants'] = []

  scrape.get_restaurants_in_region(region)
  q.put(scrape.result['restaurants'])
  


if __name__ == '__main__':
  r = requests.get(base_url, headers=headers)
  soup = make_soup(r.text)

  regions = get_regions(soup)
  print('There are {} regions'.format(len(regions)))

  manager = multiprocessing.Manager()
  q = manager.Queue()
  p = Pool(multiprocessing.cpu_count())

  watcher = p.apply_async(listener, (q,))

  jobs = []
  for region in regions:
    job = p.apply_async(main, (region, q))
    jobs.append(job)
  
  for job in jobs:
    job.get()
  
  q.put('kill')
  p.close()
