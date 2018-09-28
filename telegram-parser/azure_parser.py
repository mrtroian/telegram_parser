from config import keys
import requests


def get_azure_data(message):
    documents = { 'documents': [
        { 'id': '1', 'text': '%s' % (message) },
    ]}
    language_api_url = keys['url'] + "languages"
    headers   = {"Ocp-Apim-Subscription-Key": keys['key']}
    response  = requests.post(language_api_url, headers=headers, json=documents)
    languages = response.json()['documents'][0]['detectedLanguages'][0]['iso6391Name']
    # Просто возвращаем все данные в виде кортежа
    return (languages, get_azure_sentiment(message, languages), get_azure_phrases(message, languages), get_azure_entities(message))


def get_azure_sentiment(message, lang):
    documents = {'documents' : [
        {'id': '1', 'language': '%s' % (lang), 'text': '%s' % (message)},
    ]}
    sentiment_api_url = keys['url'] + "sentiment"
    headers   = {"Ocp-Apim-Subscription-Key": keys['key']}
    response  = requests.post(sentiment_api_url, headers=headers, json=documents)
    sentiments = response.json()['documents'][0]['score']
    return sentiments


def get_azure_phrases(message, lang):
    documents = {'documents' : [
        {'id': '1', 'language': '%s' % (lang), 'text': '%s' % (message)},
    ]}
    key_phrase_api_url = keys['url'] + "keyPhrases"
    headers   = {"Ocp-Apim-Subscription-Key": keys['key']}
    response  = requests.post(key_phrase_api_url, headers=headers, json=documents)
    key_phrases = ', '.join(response.json()['documents'][0]['keyPhrases'])
    return key_phrases


def get_azure_entities(message):
    documents = {'documents' : [
        {'id': '1', 'text': '%s' % (message)},
    ]}
    entity_linking_api_url = keys['url'] + "entities"
    headers   = {"Ocp-Apim-Subscription-Key": keys['key']}
    response  = requests.post(entity_linking_api_url, headers=headers, json=documents)
    entities = ', '.join(response.json()['documents'][0]['entities'])
    # Если entities не пустой массив, возвращаем данные по ключу name
    if entities:
        return entities[0]['name']
    # В противном случае, ссылку на пустой массив
    return entities