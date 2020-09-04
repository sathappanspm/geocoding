import json
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV
from sklearn.svm import SVC
from sklearn.ensemble import AdaBoostClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction import DictVectorizer
import ipdb

with open("./supervisedgeo_traininingdata_withpersonsIncluded.mjson") as inf:
    xmat, ymat, persmat = [], [], []
    for ln in inf:
        x, y, pers = json.loads(ln)
        xmat += x
        ymat += y
        persmat += pers

xtr, xte, ytr, yte = train_test_split(xmat, ymat, test_size=0.3, random_state=42)


def flatten(arr):
    return [_ for tmp in arr for _ in tmp]

vect = DictVectorizer()
xtrf = vect.fit_transform(flatten(xtr))
xtef = vect.transform(flatten(xte))
ytrf = flatten(ytr)
ytef = flatten(yte)

# specify parameters and distributions to sample from
from scipy.stats import randint as sp_randint

param_dist = {"n_estimators": [10, 50, 100],
    "max_depth": [3, None],
              "max_features": sp_randint(1, 11),
              "min_samples_split": sp_randint(2, 11),
              "bootstrap": [True, False],
              "criterion": ["gini", "entropy"]}

# run randomized search
n_iter_search = 20
clf = RandomForestClassifier()
random_search = RandomizedSearchCV(clf, param_distributions=param_dist,
                                   n_iter=n_iter_search, cv=5, n_jobs=-1)
random_search.fit(xtrf, ytrf)

ipdb.set_trace()
