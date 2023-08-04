# Portfolio of sample code

1) You will find example IaaC Bicep files for Azure deployments. This is example code designed
to give you a taste of how I leverage IaaC in my practice. 
2) Databricks is all scala code. I like scala becuase of 'case classes' and static typing.
This code is raw, meaning its code I wrote as a tool to transform and load data that was living
on our ADLS2 environment.
4) Snowflake code shows how I built an Hl7 parser. The scope was narrow but functional

# Logical overview of our landscape

![Alt text](/Code/img/B.png?raw=true "Bayer Data Architecture")

Power point has a breakdown of each component

This logical view is from an another healthcare client.
They are a SaaS company that integrates several hospital systems, enriches data
and delivers to providers data that is holistic and integrated accorss several EMR's.


![Alt text](/Code/img/A.png?raw=true "Architecture")


The diagram is a workflow model that they use to streamline provider and patient 
virtual visits.

![Alt text](/Code/img/A2.png?raw=true "Architecture")


This diagram shows a very simple example of how Bicep can be used to deploy data services.

![Alt text](/Code/img/BicepArch.png?raw=true "Bicep Architecture View")



