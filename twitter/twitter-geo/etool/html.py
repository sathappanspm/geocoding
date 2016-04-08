#!/usr/bin/env python
"""
Common functions for handling HTML
"""
from bs4 import Doctype
from BeautifulSoup import BeautifulSoup, Comment
from langdetect import detect
import logs
import args as args_util
import re
from sys import exit
from lxml import html as doc_parser
from pycountry import languages
from goose import Goose, Crawler
from goose.text import StopWordsArabic
from goose.text import StopWordsChinese
from goose.text import StopWordsKorean
import requests
import gevent

log = logs.getLogger(__name__)


def validate_encoding(http_response):
    """
    Validates that the encoding for the HTML/text in the http_response is
    properly marked. If it isn't, the encoding is corrected so that we don't
    receive errors on special characters of foreign languages
    :param http_response:
    :return: http_response: response with correct encoding
    """
    encodings = requests.utils.get_encodings_from_content(http_response.content)

    if encodings and encodings[0].lower() != http_response.encoding.lower():
        log.debug('Correcting encoding %s to %s' % (http_response.encoding, encodings[0]))
        http_response.encoding = encodings[0]

    return http_response


def strip_tags(html, node_text=True, tag=None):
    """
    Strips HTML tags provided from html and returns only the
    text content.
    """
    try:
        if html is None:
            return None

        soup = BeautifulSoup(html)
        if soup.head:
            soup.head.extract()

        # Remove all comment, script and style elements
        elements_to_remove = ['script', 'style']
        if tag in elements_to_remove:
            elements_to_remove.remove(tag)

        [script.extract() for script in soup(elements_to_remove)]

        comments = soup.findAll(text=lambda text: isinstance(text, (Comment, Doctype)))
        [comment.extract() for comment in comments]

        # Remove DOCTYPE string
        if (soup.contents[0]
                and type(soup.contents[0]) == str
                and re.match(r'DOCTYPE', soup.contents[0])):
            soup.contents[0].extract()

        # Remove extra white space
        return remove_extra_whitespace(''.join(soup.findAll(name=tag, text=node_text, strip=True)))
    except Exception, e:
        log.exception('Exception encountered when attempting to strip tags: %s' % e)


def extract_xpath(xpath, html):
    tree = doc_parser.fromstring(html)
    return tree.xpath(xpath)


def remove_extra_whitespace(text):
    if text is None:
        return None

    return re.sub(r"\s+", " ", re.sub(r"\n+", "\n", text)).strip(' \t\n\r')


def extract_meta_lang(html):
    """
    Returns the meta_lang of the HTML provided or None if unsuccessful
    :param html:
    :return: meta_lang | None
    """
    meta_lang = None

    try:
        goose_config = {'enable_image_fetching': False}

        goose = Goose(goose_config)
        article = goose.extract(raw_html=html)
        meta_lang = article.meta_lang
    except IOError, io:
        log.info('Error retrieving the meta language from Goose: %s' % io)
    except Exception, e:
        log.warn('Error retrieving the meta language from Goose %s: %s' % (type(e), e))

    return meta_lang


def detect_language(text):
    """
    Returns the IANA language code of the text provided
    :param text:
    :returns: string containing the natural language the text is written in
              or None if error is encountered
    """
    language = None

    try:
        language = detect(text)
        log.debug('Language is %s' % language)
    except Exception, e:
        log.warn('Error retrieving the language from langdetect for %s' % e)

    return language


def translate_iana_language_code_to_iso_639_3(iana_lang_code):
    """
    Translates the 2 character iana_lang_code provided into the appropriate
    ISO-639 3 character language code
    :param iana_lang_code:
    :return: str corresponding ISO-639 3 character language code
    """
    iso_lang_code = iana_lang_code

    try:
        language = languages.get(iso639_1_code=iana_lang_code)
        iso_lang_code = language.iso639_3_code
    except Exception, e:
        log.error('Error retrieving ISO-639 language code: %s', e)

    return iso_lang_code


def extract_content_and_language(html, url=None):
    """
    Determines language and then extracts the article object of the http_response provided
    :param html: HTML content from a URL request
    :param url: URL for the HTML provided
    :returns: dict object containing the embersLang, content, content_title,
              image and video URLS if found
    """
    result = {}

    # Using the HTML meta-lang is more reliable when attempting to extract content from HTML
    language = extract_meta_lang(html)

    # But the meta_lang can be set incorrectly or not set at all
    if language is None or language == 'en':
        log.debug('Validating HTML meta language retrieved: %s' % language)
        # Strip tags so that the English in the HTML doesn't throw off language detection
        text = strip_tags(html)
        lang_detected = detect_language(text)

        if lang_detected:
            language = lang_detected

    # Some websites incorrectly mark spanish as 'sp'
    if language == 'sp':
        language = 'es'

    if language is not None:
        result['embersLang'] = translate_iana_language_code_to_iso_639_3(language)

    result.update(extract_content(html, language=language, url=url))

    return result


def extract_content(html, language=None, url=None):
    """
    Extracts the article object of the http_response provided
    :param html: HTML content from a URL request
    :param language: optional language of the HTML content
    :param url: URL for the HTML provided
    :returns: dict object containing the content, content_title, image and video URLS if found
    """
    result = {}
    try:
        # Disabling image_fetching for now as it tends to cache a lot and
        # fill up the /tmp directory very quickly
        goose_config = {'enable_image_fetching': False}

        if language is not None:
            # Goose generalizes spanish languages for some reason
            if language in {'ca', 'pt', 'sp'}:
                language = 'es'

            # Goose requires special configuration for Arabic, Chinese, Korean
            # See https://github.com/grangier/python-goose/blob/develop/README.rst
            if language == 'ar':
                goose_config['stopwords_class'] = StopWordsArabic
            elif language == 'zh':
                goose_config['stopwords_class'] = StopWordsChinese
            elif language == 'ko':
                goose_config['stopwords_class'] = StopWordsKorean
            elif language != 'en':
                # Goose can't find the article if stop words and target_language is defined
                goose_config['use_meta_language'] = False
                goose_config['target_language'] = language

        article = None

        try:
            goose = Goose(goose_config)
            log.debug('goose_config %s' % goose_config)
            article = goose.extract(raw_html=html)
            log.debug('goose content= %s' % article.cleaned_text)
        except IOError:
            warning_message = 'Goose does not support the language %s' % language
            if url:
                warning_message += ' for url %s' % url
            log.warn(warning_message)
        except Exception, e:
            warning_message = 'Goose library could not extract article'
            if url:
                warning_message += ' for url %s' % url
            log.warn(warning_message + ': %s', e)

        if article is not None:
            result['content'] = article.cleaned_text
            if result['content'] is None or result['content'] == "":
                if article.meta_description is not None:
                    result['content'] = article.meta_description

            if article.title is not None:
                result['content_title'] = article.title

            if article.top_image is not None:
                result['content_image_url'] = article.top_image.src

            if article.movies is not None:
                for movie in article.movies:
                    if 'content_video_urls' not in result:
                        result['content_video_urls'] = []
                    result['content_video_urls'].append(movie.src)

            if hasattr(article, 'metas') and article.metas != {}:
                result['metaInfo'] = article.metas

            if hasattr(article, 'tweets') and article.tweets != []:
                result['tweetInfo'] = article.tweets

            if hasattr(article, 'links') and article.links != {}:
                result['links'] = article.links

        if 'content' not in result or not result['content']:
            info_message = 'Goose library could not extract article'
            if url:
                info_message += ' for url %s' % url
            log.info(info_message)
            content = ''

            if article is not None:
                # Attempt to remove navigation and sidebar articles by
                # removing nodes with a lot of links
                extractor = Crawler(goose.config).get_extractor()

                for node in extractor.nodes_to_check(article.doc):
                    if not extractor.is_highlink_density(node):
                        content += ' ' + extractor.parser.getText(node)

                content = remove_extra_whitespace(content)
            else:
                content = strip_tags(html)

            result['content'] = content
    except Exception, e:
        error_message = 'Error retrieving the article'
        if url:
            error_message += ' for url %s' % url

        log.exception(error_message + ': %s' % e)

    return result


def response_length_is_okay(response, max_length=500000):
    # sometimes response.headers returns a comma separated pair of digit strings
    # so we take the first one not to throw an error
    return ((response.headers.get('content-length', [])
             and int(response.headers['content-length'].split(",")[0]) < max_length)
            or len(response.text) < max_length)


def retrieve_html(url, timeout=50, max_response_length=500000):
    """
    Makes the HTTP request to the url provided. Validates the response and returns the HTML
    :param url:
    :param timeout:
    :return: str html
    """
    html = None

    with gevent.Timeout(timeout + 1):
        http_response = requests.get(url, timeout=timeout)

    if response_length_is_okay(http_response, max_response_length):
        log.debug('Extracting content from %s' % url)

        if http_response.content:
            html = validate_encoding(http_response).text

            # We have seen some HTML with XML headers. This causes errors when parsing
            if (re.search('^<\?xml version=.+ encoding=.+\?>', html, re.IGNORECASE) is not None
                and re.search('text/html',
                              http_response.headers['content-type'],
                              re.IGNORECASE) is not None):
                log.info('Mismatching content-type, HTML, and header, XML for URL %s',
                         http_response.url)
                html = re.sub('^<\?xml version=.+ encoding=.+\?>', '', html)

        log.debug('HTML retrieved: %s', html)
    else:
        log.warn("Content of URL exceeds maximum length: %s", url)

    return html


def main():
    """
    Utility to perform actions on HTML

    Required arguments:
        -a | --action <action_name> : Action to perform. 2 valid options exist so far:
                             'retrieve-content' or 'strip-tags'
        -u | --url <url> : URL for the HTML content
    """
    exit_code = 0
    logs.init(logfile='html')
    ap = args_util.get_parser()
    ap.add_argument('-a', '--action', type=str,
                    help='Action to perform. 2 valid options exist so far: '
                         '\'retrieve-content\' or \'strip-tags\'')
    ap.add_argument('-u', '--url', type=str, help='URL for the HTML content')
    ap.add_argument('-t', '--tag', type=str, default=None,
                    help='Optional HTML tag to strip and return the content')
    ap.add_argument('--text', type=str, default=True,
                    help='Optional text within HTML tags to strip')
    args = ap.parse_args()
    assert args.action, ('--action is required. The valid options are: '
                         '\'retrieve-content\' or \'strip-tags\'')
    assert args.url, '--url is required'

    try:
        html = retrieve_html(args.url)

        if html is None:
            print 'HTML was not retrieved'
            exit_code = 1
            exit(exit_code)

        if args.action == 'retrieve-content':
            contents = extract_content_and_language(html, url=args.url)

            if contents is None:
                print 'Content could not be retrieved. Check log.'
            else:
                for content in contents:
                    print content, ' = ', contents[content]
        elif args.action == 'strip-tags':
            print strip_tags(html, node_text=args.text, tag=args.tag)
        else:
            log.error('Invalid action provided: \'%s\'' % args.action)
            print 'Invalid action provided: \'%s\'' % args.action
            print 'Must be either \'retrieve-content\' or \'strip-tags\''
            exit_code = 2
    except Exception as e:
        log.exception('Exception encountered: %s' % e)
        exit_code = 1

    exit(exit_code)

if __name__ == '__main__':
    exit(main())
