Un enregistrement utilise le format ">H 30s f". Quelle est sa taille en bytes ? Détaille le calcul.
- H=ushort 2bytes, 30s= char[] 30bytes, f=float 4bytes => 36bytes

Tu lis un fichier binaire et tu obtiens les bytes b'\x00\x00\x00\x0A'. Quelle est la valeur si on l'interprète comme un int32 big-endian ? Et en little-endian ?
- big endiant => 10
- little endiant => 0A 00 00 00 = 10 * 16^6

Pourquoi est-ce qu'on utilise rstrip(b'\x00') après avoir unpacké une string ? Que se passerait-il si on ne le faisait pas ?
- pour enlever le le trailing padding.

Une DB doit stocker des champs de taille variable (ex: une adresse email qui peut faire 10 ou 200 chars). Quelles sont les deux grandes stratégies pour gérer ça ? Quels sont les trade-offs ? (Pas besoin de code, réfléchis conceptuellement)
- on peux garder la plus grande capacité c'est à dire 200 chars, mais si l'adresse email fait beacoup moins que 200 chars, on aura réservé 200bytes pour rien.
- la deuxieme stratégie est de calculer la longueur de l'adresse email et réserver la longeur exact. l'inconvénient est que on aura une longueur dynamique qui complexierait le calcul pour retrouver le record puisque les records n'onront pas la méme taille.

Pourquoi SQLite et PostgreSQL ont-ils choisi big-endian pour leur format de fichier, alors que la plupart des CPU modernes sont little-endian ?
- je ne sais pas


# Day 3

Une page fait 4096 bytes. Le header fait 8 bytes. Les records font 36 bytes chacun (ton schema du Cours 1). Combien de records maximum peut contenir cette page ? Montre le calcul.
- free_space = 4096 - 8 = 4088 bytes
 num_records_max = 4088//36 = 113 records

Qu'est-ce que le flag dirty sur une page ? Pourquoi est-ce important de le tracker ?
- il permet de marquer qu'une page a été modifiée avant la lecture. 
- pour permettre d'update la page sur le disk

Dans notre delete(), on écrase le record avec des zéros mais on ne décrémente pas num_records. Quel problème ça crée ? Comment le slotted page (avec les tombstones) résout ce problème mieux que notre implémentation actuelle ?
- c'est de l'espace occupé sur le disk qu'on ne libére pas.
- comme chaque slot a cette information sur l'offset et la longueur, lors de la suppression, on pourra enlever l'élément supprimé de la mémoire et update chaque slot.


Pourquoi est-ce qu'une page est l'unité atomique de lecture/écriture, et pas le record individuel ? Qu'est-ce que ça implique si la machine crashe au milieu de l'écriture d'une page ?
- Parce qu'on veux lire par bloc sur le disk, et que la durée de lecture sur le disk est imprtant. si la machine crashe, rien n'est mis en mémoire

Le RID encode (page_id, slot_id). Si on décide de réorganiser les records dans une page (pour compacter l'espace après des suppressions), qu'est-ce qui se passe avec les RIDs existants ? Quel impact ça a sur les indexes ?
- les slots de devront pas bouger donc même RIDs. les indexes stockent des RIDs donc pareil ils ne devront pas changer aprés une suppréssion. 


# day4:

Le Buffer Pool a 4 frames. Les pages 1, 2, 3, 4 sont chargées dans cet ordre. Toutes ont pin_count = 0. On demande maintenant la page 5. Quelle page est évincée par LRU ? Pourquoi ?
- la page 1 est évincée. parce que c'est la moins récemment utilisée.

Qu'est-ce qu'un cache hit vs un cache miss ? Quel est l'impact en termes de performance ?
- cache hit -> la valeur se trouve en RAM, et cache miss -> elle n'est pas dans la RAM, il faut donc aller le chercher dans le disk. 
- sur la performance, l'accessibilité en RAM est plus rapide que celui du disk

Pourquoi est-ce qu'on doit écrire une page dirty sur disque avant de l'évincer, et pas après ?
- les modifications seront perdues

LRU suppose que "récemment utilisé = probablement réutilisé bientôt". Donne un exemple concret où cette hypothèse est fausse dans le contexte d'une DB. (Hint: pense à un full table scan)
- table scan => fetch per page_id => for each fetch -> unpin -> en terme de performance c'est pas génial. parce que chaque page fetch entraine un update pool, même si la page ne sera probablement pas réutilisé plutard.

Dans notre implémentation, unpin_page prend un paramètre is_dirty. Pourquoi est-ce que c'est l'appelant qui décide si la page est dirty, plutôt que le Buffer Pool lui-même ?
- le buffer pool n'a pas l'information si la page est modifiée ou pas. La séparation des concerns: son rôle est de cache la page et d'écrire sur disk si c'est dirty.

# day5:

Avec order=2 (max_keys=4), insère à la main les clés [3, 7, 1, 5, 9, 2] une par une. Dessine l'arbre après chaque insertion qui provoque un split. (Pas de code, juste le dessin)
insert 3:
[3]
insert 7
[3 | 7]
insert 1
[1 | 3 | 7]
insert 5
     [3]
[1]       [5 | 7]
insert 9
     [3]
[1]       [5 | 7 | 9]
insert 2
         [3]
[1 | 2]       [5 | 7 | 9]

Quelle est la différence entre _split_leaf et _split_internal ? Pourquoi la médiane se comporte-t-elle différemment dans les deux cas ?
dans split leaf, la médiane remonte et reste dans la feuille alors que pour split interval , la médiane remonte et disparait de la feuille

Dans _insert_recursive, on split l'enfant avant de descendre dedans. Pourquoi cette approche "split en descendant" plutôt que "split en remontant" ?
la prochaine insertion va splitter les nodes plus en haut de l'arbre.

Le range_search est O(log n + k) où k est le nombre de résultats. Explique d'où vient chaque terme.
log n pour la recherche de la feuille c'est à la descente sur l'arbre et le k pour traverser la liste chainée

Actuellement, si on insère deux fois la même clé, que se passe-t-il ? Est-ce un bug ou un comportement acceptable selon le use case ?
C'est pas un bug si on insére la même clé, c'est plus les valeurs sur ces clés qui comptent.
C'est un bug c'est sur, puisque lors d'un delete, on peux supprimer une clé par erreur. Pour la base de donnée les valeurs à ces clés sont des rids donc supprimer la mauvaise clé revient donc à supprimer un de l'arbre.
Pour une use case ou l'insertion et la range query sont les 2 seuls use case, on peux considérer qu'on pourrais avoir des clés similaires.

