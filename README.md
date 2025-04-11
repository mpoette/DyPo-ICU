# Welcome to Dypo-ICU

![Logo project](assets/img/baniere_dypo.png)

## What is Dypo-ICU?

Dypo-ICU (Dynamic Prediction of Outcome in Intensive Care Units) is an initiative to develop an algorithm capable of assessing patient severity in real-time within intensive care units. Its strength lies in the seamless integration of diverse data types:
- **Dynamic variables**: Leveraging signal analysis techniques to combine high-frequency data (1 value/s) with low-frequency data (1 value/h).
- **Static variables**: Providing complementary context to enhance predictive accuracy.

## Work Packages

1. **Data Extraction**
    - Focused on building a dataset aligned with the FAIR principles (Findable, Accessible, Interoperable, Reusable).
    - A comprehensive list of variables and their descriptions is available [here].

2. **Data Preprocessing**
    1. **Data Imputation**  
        - Multivariate time series analysis often relies on algorithms like RNNs and CNNs, which are not inherently designed to handle missing data. However, missing data is a common challenge in ICU temporal datasets.
        - Various imputation methodologies are well-documented in the literature.
        - Detailed analysis of imputation techniques: [link](https://github.com/mpoette/DyPo-ICU/tree/main/4.1.imputation).
        - Beyond filling gaps, imputation can support tasks such as synthetic data generation and outlier detection.
    2. **Outlier Detection**
        - Identifying anomalies in the data to ensure robustness and reliability in downstream analyses.