#!/usr/bin/env python3
"""
aws_ec2_scheduler_setup.py
==========================
One-time setup script — run on your LOCAL PC (not on EC2).

What it creates
---------------
  1. An IAM Role  (EC2-StartStop-SchedulerRole)
       Trust policy : scheduler.amazonaws.com can assume it
       Inline policy: ec2:StartInstances + ec2:StopInstances
                      scoped to YOUR specific instance ARN only

  2. Two EventBridge Scheduler schedules (Asia/Kolkata timezone):
       EC2-Start-MarketOpen  → cron(45 08 ? * MON-FRI *)  → 08:45 IST
       EC2-Stop-MarketClose  → cron(00 16 ? * MON-FRI *)  → 16:00 IST

Idempotent
----------
  Running the script a second time is safe — it updates existing
  resources instead of crashing with "AlreadyExists" errors.

Pre-requisites
--------------
  pip install boto3

  Configure AWS credentials on your local PC ONCE before running:
      aws configure
      # Prompts:
      #   AWS Access Key ID     → your IAM user key
      #   AWS Secret Access Key → your IAM user secret
      #   Default region        → ap-south-1  (Mumbai)
      #   Default output format → json
  Your IAM user needs: IAMFullAccess + AmazonEventBridgeSchedulerFullAccess
  + AmazonEC2ReadOnlyAccess  (or AdministratorAccess for quick setup).

Usage
-----
  python aws_ec2_scheduler_setup.py
"""

import json
import sys

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    sys.exit(
        "ERROR: boto3 is not installed.\n"
        "       Run:  pip install boto3"
    )

# ─────────────────────────────────────────────────────────────────────────────
#  Fixed names — changing these after first run creates duplicate resources
# ─────────────────────────────────────────────────────────────────────────────
ROLE_NAME      = "EC2-StartStop-SchedulerRole"
SCHEDULE_GROUP = "default"          # built-in group, always exists in every account
START_SCHEDULE = "EC2-Start-MarketOpen"
STOP_SCHEDULE  = "EC2-Stop-MarketClose"
TIMEZONE       = "Asia/Kolkata"

# EventBridge Scheduler "universal target" ARNs — these call AWS SDK actions directly.
# Format: arn:aws:scheduler:::aws-sdk:{service}:{camelCaseAction}
START_TARGET_ARN = "arn:aws:scheduler:::aws-sdk:ec2:startInstances"
STOP_TARGET_ARN  = "arn:aws:scheduler:::aws-sdk:ec2:stopInstances"


# ═════════════════════════════════════════════════════════════════════════════
#  USER INPUT
# ═════════════════════════════════════════════════════════════════════════════
def prompt_inputs() -> tuple[str, str]:
    print()
    print("═" * 60)
    print("  AWS EC2 Auto-Scheduler Setup")
    print("  (EventBridge Scheduler — Asia/Kolkata)")
    print("═" * 60)

    instance_id = input("\n  EC2 Instance ID  (e.g. i-0abc123def456789a): ").strip()
    if not instance_id.startswith("i-") or len(instance_id) < 10:
        sys.exit("ERROR: That doesn't look like a valid Instance ID (must start with 'i-').")

    region_input = input("  AWS Region       [ap-south-1]: ").strip()
    region = region_input if region_input else "ap-south-1"

    print()
    print(f"  Instance : {instance_id}")
    print(f"  Region   : {region}  (Mumbai = ap-south-1)")
    print()
    confirm = input("  Proceed? [y/N]: ").strip().lower()
    if confirm != "y":
        print("  Aborted.")
        sys.exit(0)

    return instance_id, region


# ═════════════════════════════════════════════════════════════════════════════
#  IAM  — role + inline policy
# ═════════════════════════════════════════════════════════════════════════════
def ensure_iam_role(iam, instance_id: str, account_id: str, region: str) -> str:
    """
    Create (or update-in-place) the IAM role for EventBridge Scheduler.

    Trust policy  : allows scheduler.amazonaws.com to AssumeRole,
                    scoped to this AWS account via aws:SourceAccount condition.
    Inline policy : grants ONLY ec2:StartInstances + ec2:StopInstances
                    on the specific instance ARN — principle of least privilege.

    Returns the role ARN.
    """
    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "scheduler.amazonaws.com"
                },
                "Action": "sts:AssumeRole",
                # Scope the trust to this account so another account's
                # EventBridge Scheduler cannot assume this role.
                "Condition": {
                    "StringEquals": {
                        "aws:SourceAccount": account_id
                    }
                }
            }
        ]
    })

    # ── Create or fetch the role ──────────────────────────────────────────
    try:
        resp = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=trust_policy,
            Description=(
                "Allows EventBridge Scheduler to start/stop a specific EC2 instance. "
                "Created by aws_ec2_scheduler_setup.py"
            ),
        )
        role_arn = resp["Role"]["Arn"]
        print(f"  [IAM] Role created  → {role_arn}")

    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code != "EntityAlreadyExists":
            raise
        # Role exists — refresh trust policy in case account ID changed
        iam.update_assume_role_policy(
            RoleName=ROLE_NAME,
            PolicyDocument=trust_policy,
        )
        role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
        print(f"  [IAM] Role already exists — trust policy refreshed  → {role_arn}")

    # ── Attach / overwrite the inline policy ─────────────────────────────
    # Scoped to the exact instance ARN → no other instance can be touched.
    instance_arn = f"arn:aws:ec2:{region}:{account_id}:instance/{instance_id}"

    inline_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "StartStopSpecificInstance",
                "Effect": "Allow",
                "Action": [
                    "ec2:StartInstances",
                    "ec2:StopInstances"
                ],
                "Resource": instance_arn
            }
        ]
    })

    # put_role_policy is idempotent — creates or replaces
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="EC2-StartStop-InlinePolicy",
        PolicyDocument=inline_policy,
    )
    print(f"  [IAM] Inline policy applied  → restricted to {instance_arn}")

    return role_arn


# ═════════════════════════════════════════════════════════════════════════════
#  EventBridge Scheduler
# ═════════════════════════════════════════════════════════════════════════════
def upsert_schedule(
    scheduler,
    name: str,
    cron_expr: str,
    target_arn: str,
    input_payload: str,
    role_arn: str,
) -> None:
    """
    Create a schedule if it does not exist, or update it in-place if it does.

    FlexibleTimeWindow=OFF  → fires at the exact cron time, no drift.
    State=ENABLED           → active immediately after creation.
    """
    kwargs = dict(
        GroupName=SCHEDULE_GROUP,
        Name=name,
        # EventBridge Scheduler cron format:
        #   cron(minutes hours day-of-month month day-of-week year)
        #   '?' in day-of-month means "any" (required when day-of-week is set)
        ScheduleExpression=cron_expr,
        ScheduleExpressionTimezone=TIMEZONE,
        FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED",
        Target={
            # Universal target ARN that calls the AWS SDK action directly
            "Arn": target_arn,
            # The role EventBridge Scheduler will assume to call EC2
            "RoleArn": role_arn,
            # JSON payload passed to the SDK action — same as the boto3 API params
            "Input": input_payload,
        },
    )

    try:
        scheduler.create_schedule(**kwargs)
        print(
            f"  [Scheduler] Created  '{name}'"
            f"  →  {cron_expr}  ({TIMEZONE})"
        )

    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code != "ConflictException":
            raise
        # Schedule exists — update it to apply any changes
        scheduler.update_schedule(**kwargs)
        print(
            f"  [Scheduler] Updated  '{name}'"
            f"  →  {cron_expr}  ({TIMEZONE})"
        )


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════════════════
def main() -> None:
    instance_id, region = prompt_inputs()

    print()
    print("─" * 60)
    print("  Connecting to AWS …")

    session    = boto3.Session(region_name=region)
    iam        = session.client("iam")
    scheduler  = session.client("scheduler")

    # Resolve the AWS account ID from the current credentials
    account_id = session.client("sts").get_caller_identity()["Account"]
    print(f"  Account  : {account_id}")
    print(f"  Region   : {region}")
    print("─" * 60)

    # ── 1. IAM role ───────────────────────────────────────────────────────
    role_arn = ensure_iam_role(iam, instance_id, account_id, region)

    # ── 2. Build the input payload (same structure for Start and Stop) ────
    # This JSON is passed directly to the EC2 SDK action by EventBridge Scheduler.
    payload = json.dumps({"InstanceIds": [instance_id]})

    # ── 3. Start schedule — 08:45 IST, Monday to Friday ──────────────────
    upsert_schedule(
        scheduler,
        name          = START_SCHEDULE,
        cron_expr     = "cron(45 08 ? * MON-FRI *)",
        target_arn    = START_TARGET_ARN,
        input_payload = payload,
        role_arn      = role_arn,
    )

    # ── 4. Stop schedule — 16:00 IST, Monday to Friday ───────────────────
    upsert_schedule(
        scheduler,
        name          = STOP_SCHEDULE,
        cron_expr     = "cron(00 16 ? * MON-FRI *)",
        target_arn    = STOP_TARGET_ARN,
        input_payload = payload,
        role_arn      = role_arn,
    )

    # ── Summary ───────────────────────────────────────────────────────────
    print()
    print("═" * 60)
    print("  Setup complete!")
    print()
    print(f"  Instance  :  {instance_id}")
    print(f"  Timezone  :  {TIMEZONE}")
    print()
    print("  Schedule:")
    print("    Mon–Fri  08:45 IST  →  EC2 START  (15 min before market open)")
    print("    Mon–Fri  16:00 IST  →  EC2 STOP   (30 min after market close)")
    print()
    print("  Verify in AWS Console:")
    print("    https://console.aws.amazon.com/scheduler/home")
    print("═" * 60)
    print()


if __name__ == "__main__":
    main()
