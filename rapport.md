# Rapport — Nettoyage et analyse des ventes

## Objectif

Traiter le fichier de ventes avec Apache Spark SQL et l'API DataFrame de Scala, controler la qualite des lignes, calculer les montants commerciaux, enrichir les dates, segmenter les paniers et exporter les resultats par annee et mois.

## Donnees source

Le fichier contient 80 transactions et 11 colonnes : identifiants de transaction et client, date, produit, categorie, ville, quantite, prix unitaire, remise, cout unitaire et mode de paiement. Les transactions couvrent la periode du 2 janvier au 27 octobre 2026. Les identifiants de transaction sont uniques.

Quelques libelles de produit, categorie et ville utilisent des casses differentes. Le pipeline retire les espaces de bord, met les codes produit et moyens de paiement en majuscules et normalise la casse des categories et villes. Les conversions de types sont controlees ; les lignes invalides sont ecartees selon les criteres de l'enonce.

Sur le fichier fourni, les 80 lignes possedent leurs champs et leurs valeurs numeriques/dates respectent les criteres. Le bilan attendu est donc de 80 lignes conservees et 0 rejetee.

## Calculs

- `gross_amount = quantity × unit_price` ;
- `net_amount_ht = gross_amount × (1 - discount_rate)` ;
- `total_cost = quantity × unit_cost` ;
- `margin = net_amount_ht - total_cost` ;
- `margin_rate = margin / net_amount_ht × 100` ;
- `sale_segment` : `GROS_PANIER` a partir de 1 000, `PANIER_MOYEN` a partir de 300, sinon `PETIT_PANIER` ;
- `margin_status` : marge positive, negative ou nulle ;
- `year`, `month` et `day_of_week` sont extraits de l'horodatage.

Les montants sont arrondis a deux decimales. Le taux de marge d'une vente dont le chiffre d'affaires net est nul est null.

## Resultats de reference

| Indicateur | Resultat |
| --- | ---: |
| Transactions conservees | 80 |
| Chiffre d'affaires brut | 65 898,00 |
| Chiffre d'affaires net HT | 61 443,55 |
| Cout total | 53 515,61 |
| Marge totale | 7 927,94 |
| Taux de marge global pondere | 12,90 % |

| Segment | Transactions |
| --- | ---: |
| GROS_PANIER | 18 |
| PANIER_MOYEN | 21 |
| PETIT_PANIER | 41 |

Le chiffre d'affaires net HT par categorie est de 30 755,50 pour Informatique, 23 187,90 pour Telephonie, 6 145,40 pour Audio et 1 354,75 pour Accessoires.

## Conclusion

Les ventes Informatique et Telephonie portent l'essentiel du chiffre d'affaires net de cet echantillon. Les resultats de marge sont positifs au total. Le fichier est petit (80 lignes) et couvre moins d'une annee complete ; ces montants decrivent uniquement les transactions fournies et ne permettent pas, a eux seuls, de conclure sur une tendance annuelle ou sur l'ensemble de l'activite.

## Reproductibilite

Le rapport est fonde sur `data/ventes.csv`. Les indicateurs sont egalement affiches par l'application Scala lors de son execution. L'export CSV partitionne est genere dans le repertoire passe en second argument.
