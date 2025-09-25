import requests
import html
import re


def extract_google_maps_names(url_suffix: str = '!3m1!4b1!4m3!11m2!2surLpP2IDzhnAsYrsgACRFjdCIiYM4A!3e3?entry=tts&g_ep=EgoyMDI1MDEyOC4wKgBIAVAD?ucbcb=1') -> str:
    ''' 
    Extract restaurant names from a Google Maps URL. 
    This extraction method is based on this https://gist.github.com/ByteSizedMarius/8c9df821ebb69b07f2d82de01e68387d

    :param url_suffix: The specific URL suffix for the Google Maps data.
    :type url_suffix: str, optional
    :default url_suffix: A predefined suffix for my restaurant list
    :returns: A list of extracted restaurant names
    '''
    # Fetch data from google maps list url
    response = requests.get(f"https://google.com/maps/@/data={url_suffix}")
    response.raise_for_status()
    raw_text = response.text

    # Select only the restaurant data
    cleaned_text = raw_text.split(r")]}'\n")[2].split("]]\"],")[0] + "]]"
    unescaped_text = html.unescape(cleaned_text)

    # Extract only restaurant names using regex
    restaurants_names_list = re.findall(
        r'(?:\\"/g/[^\\"]++\\"]|]]),\\"(.*?)\\",\\"', unescaped_text)

    print(f"Restaurant Names List: {restaurants_names_list}")

    return restaurants_names_list
