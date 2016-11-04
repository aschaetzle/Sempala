'''
Created on 08.03.2014

@author: neua
'''


def replaceSpecialChars(pred):
    pred = pred.strip()
    pred = pred.replace(':', '_');
    pred = pred.replace('<', '_');
    pred = pred.replace('>', '_');
    pred = pred.replace('/', '_');
    pred = pred.replace('~', '_');
    pred = pred.replace('#', '_');
    pred = pred.replace('.', '_');
    pred = pred.replace('-', '_');
    pred = pred.strip()
    if pred[0] == '_':
        pred = pred[1:]
    return pred;