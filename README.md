## A Quantitative Discourse Analysis of Asian Workers in the US Historical Newspapers
This repository contains materials for "A Quantitative Discourse Analysis of Asian Workers in the US Historical Newspapers"

[Paper](https://arxiv.org/abs/2402.02572)

## Citation

:warning:
<mark style='background-color:red'>
<span style='color:red'>
*Warning: This paper contains examples of offensive language targeting marginalized population.*
</span>
</mark>

```
@inproceedings{park2023quantitative,
  title={A Quantitative Discourse Analysis of Asian Workers in the US Historical Newspapers},
  author={Park, Jaihyun and Cordell, Ryan},
  booktitle={The Joint 3rd International Conference on Natural Language Processing for Digital Humanities and 8th International Workshop on Computational Linguistics for Uralic Languages},
  pages={7},
  year={2023}
}
```
## Abstract
The digitization of historical texts invites researchers to explore the large-scale corpus of historical texts with computational methods. In this study, we present computational text analysis on a relatively understudied topic of how Asian workers are represented in historical newspapers in the United States. We found that the word ``coolie`` was semantically different in some States (e.g., Massachusetts, Rhode Island, Wyoming, Oklahoma, and Arkansas) with the different discourses around coolie. We also found that then-Confederate newspapers and then-Union newspapers formed distinctive discourses by measuring over-represented words. Newspapers from then-Confederate States associated coolie with slavery-related words. In addition, we found Asians were perceived to be inferior to European immigrants and subjected to the target of racism. This study contributes to supplementing the qualitative analysis of racism in the United States with quantitative discourse analysis.

## RQ 1: How different are the semantic meaning of ``coolie`` in each State?
The code for this research question is available in the [RQ1-word2vec-token.ipynb](https://github.com/park-jay/coolie/blob/main/RQ1-word2vec-token.ipynb)

## RQ 2: What are the words over-represented in the newspapers between then-Confederate States and then-Union States?
The code for this research question is available in the [RQ2-log-odds-token.ipynb](https://github.com/park-jay/coolie/blob/main/RQ2-log-odds-token.ipynb) 

Please note that [RQ2-redo-log-odds-token.ipynb](https://github.com/park-jay/coolie/blob/main/RQ2-redo-log-odds-token.ipynb) has been modified to group the States into slave-States and free-States, not then-Confederate States and then-Union States.

## RQ 3: What ``coolie`` stories are reprinted and what are their characteristics?
The code for this research question is available in the [RQ3-reprint-viz.ipynb](https://github.com/park-jay/coolie/blob/main/RQ3-reprint-viz.ipynb)
