## 1/ On travaille TOUJOURS dans un virtualenv
````
virtualenv -p python3 py37-labs
````
ou 
````
python -m venv py37-labs
conda create --name py37-env python=3.7
````
Puis (sous Linux)
````
cd env
. ./bin/activate
````

## 2/ Créons notre strucure de projet
Structure du projet classique...
* README => fortement suggéré par gitlab / github
* LICENSE => fortement suggéré par gitlab / github
* .gitignore => prennons celui de github pour le langage python et ajoutons les spécificités de nos éditeurs et évitons de commiter notre vitualenv
* setup.py et setup.cfg pour installer notre projet
* requirements.txt
* requirements-dev.txt si nécessaire
* doc
* tests
* et les modules...

On installe le projet en local avec pip. Pour pouvoir bosser sans réinstaller, on utilise l'option -e (--editable), le mode développement :
````
pip install -e .
````

Et les dépendance de développement (pytest, black)
````
pip install -r requirements-dev.txt
````

## 3/ Installons un éditeur de code
PyCharm : https://www.jetbrains.com/pycharm/download/
ou VSCode

## 4/ Le main du projet...

Le main est dans le fichier qualify_referers.py

Et on l'ajoute au setup.cfg :
````
[options.entry_points]
console_scripts =
    qualify_referers = qualify_referers:main_method
```` 
Et on réinstalle
````
pip install -U -e .
````

