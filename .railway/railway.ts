import { defineRailway, project, service } from "railway/iac";

// finance-subscriptions-api: the Subscriptions cell. Variables stay unmanaged (secrets live in Railway only).
// subscriptions-api sleeps when idle (Railway Serverless) and wakes on the next request.
// Every service runs in Singapore next to Neon (aws-ap-southeast-1) and empire-platform; the IaC does not
// carry the region, so after the first deploy: railway scale --service <svc> asia-southeast1-eqsg3a=1 us-east4-eqdc4a=0
// Every service deploys from GitHub main after CI passes (Railway Wait for CI, 2026-10-18).
// Declared here so `railway config apply` keeps the connection instead of removing it.
const github = {
  type: "github" as const,
  repo: "bookchaowalit/finance-subscriptions-api",
  branch: "main",
  checkSuites: true,
};

export default defineRailway(() => {
  const api = service("subscriptions-api", {
    source: github,
    healthcheck: "/health",
    healthcheckTimeout: 60,
    deploy: { sleepApplication: true },
  });

  return project("finance-subscriptions-api", {
    variables: { managed: false },
    resources: [api],
  });
});
