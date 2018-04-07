#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog
"""

__author__ = ['Patrick Butler', 'Rupinder Paul']
__email__ = ['pabutler@vt.edu', 'rupen@cs.vt.edu']
__version__ = "0.0.1"


MENA_COUNTRIES = set(['Egypt', 'Iraq', 'Libya', 'Jordan', 'Bahrain', 'Syria', 'Saudi Arabia'])

NON_MENA_COUNTRIES = set(['Andorra', 'United Arab Emirates', 'Afghanistan', 'Antigua and Barbuda',
 'Anguilla', 'Albania', 'Armenia', 'Angola', 'Antarctica', 'Argentina', 'American Samoa', 'Austria',
 'Australia', 'Aruba', 'Aland Islands', 'Azerbaijan', 'Bosnia and Herzegovina', 'Barbados',
 'Bangladesh', 'Belgium', 'Burkina Faso', 'Bulgaria', 'Burundi', 'Benin', 'Saint Barthelemy', 'Bermuda',
 'Brunei', 'Bolivia', 'Bonaire, Saint Eustatius and Saba ', 'Brazil', 'Bahamas', 'Bhutan', 'Bouvet Island',
 'Botswana', 'Belarus', 'Belize', 'Canada', 'Cocos Islands', 'Democratic Republic of the Congo',
 'Central African Republic', 'Republic of the Congo', 'Switzerland', 'Ivory Coast', 'Cook Islands', 'Chile',
 'Cameroon', 'China', 'Colombia', 'Costa Rica', 'Cuba', 'Cape Verde', 'Curacao', 'Christmas Island',
 'Cyprus', 'Czech Republic', 'Germany', 'Djibouti', 'Denmark', 'Dominica', 'Dominican Republic', 'Algeria',
 'Ecuador', 'Estonia', 'Western Sahara', 'Eritrea', 'Spain', 'Ethiopia', 'Finland', 'Fiji',
 'Falkland Islands', 'Micronesia', 'Faroe Islands', 'France', 'Gabon', 'United Kingdom', 'Grenada', 'Georgia',
 'French Guiana', 'Guernsey', 'Ghana', 'Gibraltar', 'Greenland', 'Gambia', 'Guinea', 'Guadeloupe', 'Equatorial Guinea',
 'Greece', 'South Georgia and the South Sandwich Islands', 'Guatemala', 'Guam', 'Guinea-Bissau', 'Guyana',
 'Hong Kong', 'Heard Island and McDonald Islands', 'Honduras', 'Croatia', 'Haiti', 'Hungary',
 'Indonesia', 'Ireland', 'Israel', 'Isle of Man', 'India', 'British Indian Ocean Territory', 'Iran', 'Iceland',
 'Italy', 'Jersey', 'Jamaica', 'Japan', 'Kenya', 'Kyrgyzstan', 'Cambodia', 'Kiribati',
 'Comoros', 'Saint Kitts and Nevis', 'North Korea', 'South Korea', 'Kosovo', 'Kuwait',
 'Cayman Islands', 'Kazakhstan', 'Laos', 'Lebanon', 'Saint Lucia', 'Liechtenstein', 'Sri Lanka', 'Liberia',
 'Lesotho', 'Lithuania', 'Luxembourg', 'Latvia', 'Morocco', 'Monaco', 'Moldova', 'Montenegro',
 'Saint Martin', 'Madagascar', 'Marshall Islands', 'Macedonia', 'Mali', 'Myanmar', 'Mongolia', 'Macao',
 'Northern Mariana Islands', 'Martinique', 'Mauritania', 'Montserrat', 'Malta', 'Mauritius', 'Maldives',
 'Malawi', 'Mexico', 'Malaysia', 'Mozambique', 'Namibia', 'New Caledonia', 'Niger', 'Norfolk Island',
 'Nigeria', 'Nicaragua', 'Netherlands', 'Norway', 'Nepal', 'Nauru', 'Niue', 'New Zealand',
 'Oman', 'Panama', 'Peru', 'French Polynesia', 'Papua New Guinea', 'Philippines', 'Pakistan', 'Poland',
 'Saint Pierre and Miquelon', 'Pitcairn', 'Puerto Rico', 'Palestinian Territory', 'Portugal', 'Palau',
 'Paraguay', 'Qatar', 'Reunion', 'Romania', 'Serbia', 'Russia', 'Rwanda', 'Solomon Islands',
 'Seychelles', 'Sudan', 'South Sudan', 'Sweden', 'Singapore', 'Saint Helena', 'Slovenia', 'Svalbard and Jan Mayen',
 'Slovakia', 'Sierra Leone', 'San Marino', 'Senegal', 'Somalia', 'Suriname', 'Sao Tome and Principe',
 'El Salvador', 'Sint Maarten', 'Swaziland', 'Turks and Caicos Islands',
 'Chad', 'French Southern Territories', 'Togo', 'Thailand', 'Tajikistan', 'Tokelau', 'East Timor', 'Turkmenistan',
 'Tunisia', 'Tonga', 'Turkey', 'Trinidad and Tobago', 'Tuvalu', 'Taiwan', 'Tanzania', 'Ukraine',
 'Uganda', 'United States Minor Outlying Islands', 'United States', 'Uruguay', 'Uzbekistan', 'Vatican',
 'Saint Vincent and the Grenadines', 'Venezuela', 'British Virgin Islands', 'U.S. Virgin Islands', 'Vietnam', 'Vanuatu', 'Wallis and Futuna',
 'Samoa', 'Yemen', 'Mayotte', 'South Africa', 'Zambia', 'Zimbabwe', 'Serbia and Montenegro',
 'Netherlands Antilles'])
