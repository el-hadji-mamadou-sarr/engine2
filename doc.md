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

