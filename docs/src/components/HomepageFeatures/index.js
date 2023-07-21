import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Fun to Use',
    Svg: require('@site/static/img/easy.svg').default,
    description: (
      <>
        Designed with Developer Experience at front of mind. All fancy type shenanigans are hidden away, allowing you to focus on app logic.
      </>
    ),
  },
  {
    title: 'Composable',
    Svg: require('@site/static/img/composable.svg').default,
    description: (
      <>
        Composable middleware framework lets you know exactly what is happening at each step of the request lifecycle.
      </>
    ),
  },
  {
    title: 'Pluggable Backends',
    Svg: require('@site/static/img/plug.svg').default,
    description: (
      <>
        Works with any backend, including but not limited to: Express, AWS HTTP API, AWS REST API. Completely extensible to work with any backend.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
