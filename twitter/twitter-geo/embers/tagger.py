#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'

import Levenshtein

from embers import geocode
from embers import utils

flu_words = utils.normalize_str(
          ("gripe, influenza, antivirales, fiebre, síntomas, tos, fatiga, "
           "dolor de garganta, estornudos, dolor de cabeza, antibióticos, "
           "Oseltamivir, Tamiflu, Tazamir, neumonía, "
           "intensa falta de respiración, náusea, infección, enfermedades, "
           "vómitos, escalofríos, medicina, medicamento, vacuna, enfermedad, "
           "enfermos, médico, clínica, hospital").decode("utf8")).split(", ")

flu_words += utils.normalize_str(
        ("flu, influenza, antiviral, fever, symptoms, cough, fatigue, "
         "sore, throat, sneezing, headache, antibiotics, Tamiflu, Tazamir,"
         "pneumonia, severe shortness of breath, nausea, infection, "
         "diseases, vomiting, chills, medicine, medicine, vaccine, disease, "
         "sick, doctor, clinic, hospital").decode("utf8")).split(", ")

flu_words = list(set(flu_words))  # make list unique
print flu_words
#possible_tags = flu_words + geocode.cnames
possible_tags = ["%s country:%s" % (word, country)
                     for country in geocode.cnames
                     for word in flu_words]
possible_tags += ["*"]
possible_tags += flu_words


def gen_tags(tweet, users, geo=None):
    if not geo:
        geo = geocode.Geo()
    tags = []
    city, country = geo.geo_normalize(tweet, users)
    ctag = None
    if city:
        tags += ["city:" + city]
    if country:
        ctag = "country:" + country
        tags += [ctag]

    text = utils.normalize_str(tweet['interaction_content']).split(" ")
    for word in text:
        for f in flu_words:
            if f == "tos" and word == f:
                tags += [f]
            elif Levenshtein.ratio(word, f) >= .75:
                tags += [f]

    tags = list(set(tags))
    if ctag:
        tags += [tag + " " + ctag for tag in tags
                         if not tag.startswith("country:")]
    return tags
