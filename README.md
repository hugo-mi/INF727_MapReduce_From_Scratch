# Implémentation de Hadoop MapReduce "from scratch" en Python

## Présentation du projet

Implémentation à partir de zéros d'une version simple du concept MapReduce avec le langage python. 

Plusieurs ordinateurs d’une salle de TP des locaux de Télécom Paris connectés en réseaux afin de paralléliser le comptage des mots contenu au sein d’un fichier texte. 

L’objectif recherché avec l’augmentation du volume de données n’est plus de centraliser les données sur une seule machine mais plutôt de distribuer le stockage et paralléliser les traitements sur plusieurs machines. Le fait de regrouper les ordinateurs entre eux crée un effet de synergie c’est-à-dire que la puissance combinée de plusieurs petits ordinateurs est supérieure à la puissance d’un gros serveur central. 
Les avantages du calcul distribué sont les suivants : 

Les avantages du calcul distribué sont les suivants : 

* Scalabilité horizontale : Pour augmenter les performances et la capacité de stockage d’un cluster (groupement de plusieurs machines), il suffit de rajouter un ordinateur au cluster qui permet donc d’ajouter très facilement un nouveau nœud au cluster. 

* Tolérance aux pannes : Pour être résilient face aux pannes (ex : pannes de nœuds très fréquents = machine qui tombe en panne, problème de connectivité entre les nœuds ou les casiers et le réseau), les données sont dupliquées sur plusieurs machines (3 fois par défaut). Dans Hadoop HDFS qui permet d’être tolérant face aux pannes.   

* Les goulots d’étranglement : Dans une architecture client/serveur où l’ensemble des données est stocké sur un serveur central, les opérations de transfert de données qui se dirigent vers le serveur central engendrent un goulot d’étranglement. Dans une architecture distribuée, la charge de travail est répartie entre les différentes machines du cluster dans lesquelles nous ne retrouvons pas ce problème de goulot d’étranglement.

## Présentation de MapReduce

La première motivation de l’introduction de MapReduce concerne le traitement des tâches dit « embarrassingly parallel ». Cela est un abus de langage car contrairement à ce que l’on pourrait croire,  « embarrassingly parallel » ne veut pas dire « embarrassant à paralléliser » mais plutôt « parallélisable à l’excès». Ainsi, MapReduce, n’a pas été conçu dans le but de paralléliser des tâches difficiles à paralléliser mais plutôt pour des tâches facilement parallélisables. En effet, MapReduce pourra fonctionner seulement sur une tâche qui peut se découper en plusieurs sous-tâches indépendantes. Cela peut être perçu comme une limite de MapReduce mais il s’avère que dans le domaine de l’analyse de données, pour la majorité des applications de traitement de données, les données ont une structure régulière offrant ainsi l’avantage d’exploiter la puissance du parallèle. 
Par ailleurs, MapReduce est un modèle algorithmique qui permet de penser le découpage d’un problème parrallélisable en plusieurs sous tâches indépendantes en suivant 4 phases. 
Pour expliquer le fonctionnement de ces phases, nous nous plaçons dans le contexte du comptage des mots d’un fichier texte.

## Implémentation d'une version simple de MapReduce

L’implémentation du projet peut être découpé en 6 étapes : 

* **Phase d’initialisation :** Déploiement de l’environnement sur chaque machine
* **Phase de Splitting :** Découpage du texte en plusieurs extraits qui seront traité par chaque machine
* **Phase de Mapping :** Construction de la paire clé/valeur
* **Phase Shuffling :** Redistribution des paires clé/valeur sur chaque machine
* **Phase de Reducing :** Regroupement des paires clé/valeur sur chaque machine
* **Phase d’Assembling :** Regroupement de toutes les paires clé/valeur préalablement regroupées par chaque machine durant la phase reducing.

## Texte d'entrée

**Input.txt**
_Deer Bear River_
_Car Car River_
_Deer Car Bear_

## Objectif
Compter les mots du fichier texte input.txt en tirant profit de la puissance de 3 machines connectées au même réseau

## Architecture Répartis

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Architecture_Repartis.png)

Les modèles de communication déterminent le ou les responsables lorsqu’une requête est adressée au cluster. Deux machines arrivent à s’échanger des informations grâce à un protocole de communication qui est un ensemble de spécifications qui permettent à deux ordinateurs de communiquer. Le modèle de communication utilisé ici suit le paradigme Maître-Esclave. A travers ce type d’architecture, les requêtes adressées au cluster (groupement des machines) sont réparties entre chaque machine esclave par une machine maître. La gestion entière du cluster est adressée à la machine maître qui connait la répartition exacte des données sur chaque machine. La communication est donc bidirectionnelle et peut être facilement chainée sur le réseau sans créer de goulot d’étranglement. De plus la centralisation rendue possible grâce à la machine maître facilite l’administration du cluster. 

De plus, le mode de partage des ressources dans l’architecture distribuée que j’ai mise en place au sein du cluster est celui du « shared nothing ». En effet, aucune ressource n’est partagée entre chacune des machines du cluster via le réseau LAN. En effet, les machines esclaves ne partagent ni la mémoire ni le disque dur. Chaque machine esclave utilise ses propres ressources.


### Execution d'une commande bash avec python sur une machines distante

`def ssh(command):
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+machine,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in [0, 1, 2]:
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
`

### Copie de fichier entre machine distante

`def scp(localPath,distantPath):
    listproc = []
    timer=5
    login="hmichel-20"
    for i in [10, 11, 13]:
        machine = "tp-1a201-"+str(i)
        proc = subprocess.Popen(["scp",localPath,login+"@"+machine+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in [0, 1, 2]:
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")    
`

### Phase de démarrage (Initialisation de l'environnement sur les 3 machines)

* `/tmp/hmichel-20` : dossier qui contient l’ensemble des dossiers de chacune des phases map, shuffling et reduce
* `/tmp/hmichel-20/splits` : dossier qui contient une partition du texte qui a été découpé en N partition
* `/tmp/hmichel-20/maps` : dossier qui contient le fichier généré par la phase Mapping. 
* `/tmp/hmichel-20/shuffles` : dossier qui contient les fichiers générés par la phase Shuffling. 
* `/tmp/hmichel-20/shufflesreceived` : dossier qui contient les fichiers avec le hash correspondant à chaque clé (mot de la portion de texte). 
* `/tmp/hmichel-20/reduces` : dossier qui contient le fichier généré par la phase Reducing.

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Deploy.png)

### Phase de File Splitting

1. Nettoyage du fichier : Suppression des lignes vides du fichier texte avec la fonction python `strip()`
2. 2.	Calcul du nombre de répartition des lignes pour chaque partition : Le nombre total des lignes du fichier d’entrée est divisé (division entière) par le nombre de machine. De cette façon on obtient un nombre de lignes identiques pour chaque partition. Mais il se peut qu’il reste encore des lignes du texte qui ne sont pas prises en compte car nous effectuons une division entière. 
`split_line = nb_line // nb_machine`
 
3. Le nombre de lignes restant est calculé avec le modulo du nombre de machine
`remaining_line = nb_line % nb_machine`

4.	Création des intervalles pour sélectionner les N lignes qui seront intégrées dans chaque partition

`int1 = split_line
int2 = 2 * split_line
int3 = 3 * split_line
…
int30 = 30*(split_line) + remaining_line`

En généralisant ça donne : 
_**int(i) = i * split_line et int_last(i) = i*(split_line) + remaining_line**_

5.	Enfin il convient à l’aide des différents intervalles calculés à l’étape 3 de créer les dataframes dont chacune des lignes du dataframe contiendra une ligne de la partition de texte. 
df1 = df[0:int1]
df2 = df[int1:int2]
df3 = df[int2:int3]
…
df30 = df[int29:int30]

En généralisant cela donne 
**_df(i) = df[int(i-1) :int(i)]_**

**Split function**

`def split_input_file():
    df = pd.read_csv('/tmp/hmichel-20/input.txt', sep="\n", header=None)
    nb_line = df.shape[0]
    
    nb_machine = 3
    
    split_line = nb_line // nb_machine
    remaining_line = nb_line % nb_machine
    
    int1 = split_line
    int3 = split_line + split_line
    int4 = split_line + split_line + split_line + remaining_line
    
    df1 = df[0:int1]
    df2 = df[int1:int3]
    df3 = df[int3:int4]
    
    df1.to_csv(r'/tmp/hmichel-20/splits/s10.txt', header=None, index=None, sep='\n', mode='w')
    df2.to_csv(r'/tmp/hmichel-20/splits/s11.txt', header=None, index=None, sep='\n', mode='w')
    df3.to_csv(r'/tmp/hmichel-20/splits/s13.txt', header=None, index=None, sep='\n', mode='w')
`

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Deploy.png)

### Phase de Mapping

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Slave_mapper.png)

### Phase de Shuflling

**Calcul du hash pour chaque paire de mots**

`word_encoded = word.encode('utf-8')
 hashcode = str(int.from_bytes(hashlib.sha256(word_encoded).digest()[:2], 'little'))
 filename_shuffled = "/tmp/hmichel-20/shuffles/" + hashcode + "-" + socket.gethostname() + ".txt"`

**Répartition des paires clé/valeur sur chaque machine**

`    nb_machines = 3
    hashcode = hashcode
    num_machine = hashcode % nb_machines
    if num_machine == 0:
        id_machine = 10
    elif num_machine == 1:
        id_machine = 11
    elif num_machine == 2:
        id_machine = 13
`

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/slave_shuffler.png)

### Phase de Reducing

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Slave_reducer.png)

### Résultat

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Output_Basic.png)

### Performance avec 3 machines

**Fichier texte "deontologie_police_nationale.txt" (8 ko)**

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Output_Basic2.png)

**Output**

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Output_Basic2_.png)

**Fichier texte "domaine_public_fluvial.txt" de 71 ko**

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Output_Basic3_.png)

**Output**

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Output_Basic3.png)


### Performance avec 30 machines

**Fichier texte "deontologie_police_nationale.txt" (8 ko)**

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Output_30.png)

**Fichier texte "domaine_public_fluvial.txt" de 71 ko**

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Output_30_.png)

## Version améliorée de MapReduce (Passage en DataFrame)

Limitation des transferts de fichiers entre les machines esclaves durant la phase de shuffling par le passage des fichiers au format csv (DataFrame). L’idée est donc de transformer les fichiers textes générés au format .txt en fichier au format csv. Cela permettra alors de diminuer le nombre de fichiers crée durant la phase de shuffling. En procédant de cette manière il sera alors nécessaire d’envoyer un seul fichier csv contenant toutes les différentes paires de clé/valeur à la machine en question. 

### Phase de Mapping

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Slave_mapper_Enhanced.png)

### Phase de Shuffling

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Slave_Shuffler_Enhanced.png)

### Phase de Reducing

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Slave_Reducer_Enhanced.png)

### Performance de la version améliorée avec 30 machines

**Fichier texte "deontologie_police_nationale.txt" (8 ko)**

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Output_Enhanced.png)

**Fichier texte "domaine_public_fluvial.txt" de 71 ko**

![](https://github.com/hugo-mi/INF727_MapReduce_From_Scratch/blob/main/Images/Output_Enhanced2.png)
