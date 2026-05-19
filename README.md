<div id="toc" align="center">
  <ul>
    <summary>
      <h1>
        <img src="docs/boom_logo.png" alt="BOOM logo" width="160">
        <br/>
        BOOM
      </h1>
    </summary>
  </ul>
  <em>Burst & Outburst Observations Monitor</em>
</div>

## Boom Filter Sandbox

This repository is the **filter sandbox variant of BOOM**, intended to run a
public, sandbox-style instance for filter development and testing.

It tracks the upstream BOOM codebase and exists only to carry the set of changes required to run that
public sandbox instance.

> ⚠️ **Important — do not deploy this variant in production.**
>
> This version intentionally exposes the **filter endpoints as public**
> (unauthenticated). That is acceptable here **only** because this instance is
> meant to be isolated and to serve **public data exclusively**.
>
> Making the filter endpoints public is something you should **absolutely
> avoid** in any production environment.

### Why this exists

The goal of this deployment is to run on a separate, dedicated instance that
handles **only public data**. This lets people develop and test their filters,
experimenting freely against real public alerts, without
consuming resources on the production BOOM instances.

- Isolated instance, public data only.
- Public (unauthenticated) filter endpoints.
- A safe playground for filter development that does not impact production BOOM.

This repository follows the upstream [BOOM project](https://github.com/boom-astro/boom). For the full project
documentation refer to the BOOM README:

- **[boom-astro/boom — README](https://github.com/boom-astro/boom/blob/main/README.md)**