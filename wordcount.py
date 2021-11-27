# -*- coding: utf-8 -*-
"""
INF 727
Projet 
Etape 1 Faire un programme séquentiel non parallélisé qui compte le nombre d'occurrences des mots dans un fichier.
"""

import sys
import time

def helper(filename): # fonction qui lit le fichier texte et le découpe en un liste de mots et retourne le nombre d'occurence de chaque mot du texte sous forme d'un dictionnaire
    word_counter = {}
    file = open(filename, 'r', encoding='utf-8') # on ouvre le fichier 
    for line in file: # on lit chaque ligne du fichier jusque qu'il n'y en ai plus
        words = line.split() # on découpe chaque ligne en un liste de mot
        for element in words:
            element = element.lower() # on initialise chaque mot en minuscule
            if not element in word_counter:
                word_counter[element] = 1 # si l'élément n'a jamais été rencontré dans le texte alors on l'ajoute dans le dictionnaire de mots et on intialise la valeur à 1 (correspond au nombre d'occurence)
            else:
                word_counter[element] = word_counter[element] + 1
    file.close()
    return word_counter

def get_counter(word_counter_tuple):
    """Retourne le nombre d'occurence d'un mot en particulier contenu dans un fichier texte"""
    print(word_counter_tuple[1])
    return word_counter_tuple[1]

def print_words(filename):
    """Affiche l'occurence de chaque mot contenu dans un fichier texte sous la forme d'un dictionnaire"""
    word_counter = helper(filename)
    words = sorted(word_counter.keys()) # on classe pas ordre alphabétique les mots du dictionnaire
    for element in words:
        print(element, word_counter[element]) # on affiche le mot (la clé) et son nombre d'occurence (valeur)
        
def print_top(filename):
  """Affiche les mots avec le plus grand occurence contenu dans un fihcier texte"""
  word_count = helper(filename)

  # On trie les mots de façon à ce que les grands nombres soient les premiers en utilisant la fonction key=get_count() pour extraire le nombre.
  items = sorted(word_count.items(), key=get_counter, reverse=True) # Reverse = True car par défaut la fonction sorted classe du plus petit au plus grand or nous voulons ici faire l'inverse

  for item in items:
    print (item[0], item[1])

def print_top_right_order(filename):
  """Affiche les mots avec le plus grand occurence contenu dans un fihcier texte"""
  start_time1 = time.time()
  word_count = helper(filename)
  print(("Etape nb occurences --- %s seconds ---\n" % (time.time() - start_time1)))

  # On trie les mots de façon à ce que les grands nombres soient les premiers en utilisant la fonction key=get_count() pour extraire le nombre.
  start_time2 = time.time()
  items = sorted(word_count.items(), key=lambda item: (-item[1], item[0])) # Reverse = True car par défaut la fonction sorted classe du plus petit au plus grand or nous voulons ici faire l'inverse
  print(("Etape Tri --- %s seconds ---\n" % (time.time() - start_time2)))

  print(("Temps execution total --- %s seconds ---\n" % (time.time() - start_time1)))

  for item in items[:1]:
    print (item[0], item[1])

def main():
  if len(sys.argv) != 3:
    print ('usage: ./wordcount.py {--count | --topcount} file')
    sys.exit(1)

  option = sys.argv[1]
  filename = sys.argv[2]
  if option == '--count':
    print_words(filename)
  elif option == '--topcount':
    print_top(filename)
  elif option == '--topcountorder':
    print_top_right_order(filename)
  else:
    print ('unknown option: ') + option
    sys.exit(1)

if __name__ == '__main__':
  main()