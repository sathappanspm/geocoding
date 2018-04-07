#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"
import unicodecsv
import os
from lxml import etree
import pdb
from parse import parse as textparse


def safe_gettext(tree_node):
    if tree_node is not None and tree_node.text:
        return tree_node.text
    return ""


def safe_delete(mydict, key):
    """
    Delete only if key present in dictionary
    """
    if key in mydict:
        del(mydict[key])

    return mydict


class XMLTAG(dict):
    def __init__(self, tag_src="./acqmedia_xmltags"):
        flist = os.listdir(tag_src)
        data = {}
        for fl in flist:
            with open(os.path.join(tag_src, fl)) as infile:
                reader = unicodecsv.reader(infile, delimiter=",")
                reader.next()
                data.update(dict([l for l in reader]))
        super(XMLTAG, self).__init__(data)

    def __getitem__(self, key):
        if not self.has_key(key):
            return key
        return super(XMLTAG, self).__getitem__(key)


class ACQUIRE_MEDIA(object):
    def __init__(self, tag_src):
        self.__xmltags__ = XMLTAG(tag_src=tag_src)

    def parse(self, fileObj):
        tree = etree.fromstring(fileObj.read())
        nitf, resources = tree.getchildren()
        head, body = nitf.getchildren()
        doc = {'head': {'title': head.getchildren()[0].text},
               'body': {}
               }
        bhead, bcontent = body.getchildren()
        doc['body']['head'] = {ch.tag : ch.text for ch in bhead.getiterator()
                               if ch.text.strip()}
        doc['body']['content'] = self._getcontent(bcontent)
        doc['resources'] = self._getmeta(resources)
        return doc

    def _figure(self, fig):
        fig_msg =  {'caption': " ".join([l.strip() for l in
                                     fig.itertext() if 'figcaption' not in l]),
                'img': [img.attrib for img in fig.findall('img')],
                'type': 'figure'
                }
        fig.getparent().remove(fig)
        return fig_msg

    def _href(self, a):
        htext = " ".join([l.strip() for l in a.xpath('descendant-or-self::text()')])
        return {'type': 'a',
                htext: a.attrib.get('href', '')}

    def _entities(self, ent):
        return {"expr": ent.text.strip(),
                "value": self.__xmltags__[ent.attrib['value']],
                "neType": ent.tag}

    def _div(self, div):
        while (len(div) == 1 and (div.text is None or
                                  div.text.strip() == "")):
            div = div.getchildren()[0]

        divcontent = self._getcontent(div, recurse_div=False)
        div.getparent().remove(div)
        return divcontent

    def _getcontent(self, ctree, recurse_div=True):
        div = map(self._div, ctree.findall('div'))
        if not recurse_div and div:
            div = map(self._combine, div)

        def gettext(it):
            return " ".join([l for l in it.xpath('descendant-or-self::text()')
                             if l.strip()])

        figures = map(self._figure, ctree.xpath("descendant::figure|img"))
        content = gettext(ctree)
        #" ".join([gettext(l).strip() for l in
                  #          ctree.xpath("descendant-or-self::*[not(ancestor-or-self::figure)]")
                  #              if l.text]).strip()
        entities = map(self._entities, ctree.xpath("descendant::*[@value]"))
        href = map(self._href, ctree.xpath("descendant::a[@href]"))
        res = {"content": content.strip(), "figures": figures,
               "entities": entities, "href": href,
                "parts": div}
        if not recurse_div and len(res.get('parts', [])) == 1:
            res = self._update(res, res['parts'][0])
            safe_delete(res, 'parts')
        return res

    def _update(self, orig_div, newd):
        orig_div['content'] += ("\n" + newd['content'])
        orig_div['figures'].extend(newd['figures'])
        orig_div['entities'].extend(newd['entities'])
        orig_div['href'].extend(newd['href'])
        return orig_div

    def _combine(self, divs):
        div = divs
        for d in divs.get("parts", []):
            d = self._combine(d)
            self._update(div, d)

        safe_delete(div, 'parts')
        return div

    def _getmeta(self, mtree):
        """
        parse through the newsedge resource tree
        """
        ns = "{http://www.xmlnews.org/namespaces/meta#}"
        req_keys = ("publicationTime", "receivedTime", "expiryTime",
                    "releaseTime", "publishReason", "releaseStatus", "revision",
                    "type", "dateline", "bylineOrg", "providerSlug", "role",
                    "language", "companyCode", "keyword", "providerSymbol",
                    "providerCode")
        req_tags = ("locationCode", "industryCode", "subjectCode")

        resource_dict = {key: safe_gettext(mtree.find("{}{}".format(ns, key))) for key in req_keys}
        tag_dict = {key: self.__xmltags__[safe_gettext(mtree.find("{}{}".format(ns, key)))]
                    for key in req_tags}

        resource_dict.update(tag_dict)
        resource_dict["link"] = None
        ltree = mtree.xpath(".//*[contains(text(), 'Story Link')]")
        if ltree:
            resource_dict["link"] = textparse("AMSPIDER:Story Link={}", ltree[0].text)[0]

        return resource_dict


if __name__ == "__main__":
    with open("./newsedge/201602210000AMSPIDERPOSTMEDN_OttCit01_bf45030c5c3088dfb5b21c93ef65c8d8_8.xml") as infile:
        aq = ACQUIRE_MEDIA(tag_src="./acqmedia_xmltags")
        ss = aq.parse(infile)
