#!/usr/bin/env python3
"""
XDEW Kubernetes Operator v2

A Kubernetes operator for managing XDEW projects and workspaces with
approval workflow, RBAC, and lifecycle management.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional, List

import kopf
import kubernetes
from kubernetes.client.exceptions import ApiException


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProjectPhase(Enum):
    """Enumeration of possible project phases."""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    TERMINATED = "terminated"


class WorkspacePhase(Enum):
    """Enumeration of possible workspace phases."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    TERMINATED = "terminated"


class Environment(Enum):
    """Enumeration of environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class Decision(Enum):
    """Enumeration of approval decisions."""
    APPROVED = "approved"
    REJECTED = "rejected"


@dataclass
class AuditEntry:
    """Audit log entry."""
    timestamp: str
    user: str
    action: str
    status: str
    comment: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "user": self.user,
            "action": self.action,
            "status": self.status,
            "comment": self.comment
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AuditEntry':
        return cls(
            timestamp=data["timestamp"],
            user=data["user"],
            action=data["action"],
            status=data["status"],
            comment=data["comment"]
        )


@dataclass
class ApprovalEntry:
    """Approval entry."""
    user: str
    decision: str
    timestamp: str
    comment: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "user": self.user,
            "decision": self.decision,
            "timestamp": self.timestamp,
            "comment": self.comment
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ApprovalEntry':
        return cls(
            user=data["user"],
            decision=data["decision"],
            timestamp=data["timestamp"],
            comment=data["comment"]
        )


@dataclass
class ResourceQuota:
    """Resource quota configuration."""
    cpu: str = "1000m"
    memory: str = "2Gi"
    storage: str = "10Gi"
    
    def to_dict(self) -> Dict[str, str]:
        return {
            "cpu": self.cpu,
            "memory": self.memory,
            "storage": self.storage
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'ResourceQuota':
        return cls(
            cpu=data.get("cpu", "1000m"),
            memory=data.get("memory", "2Gi"),
            storage=data.get("storage", "10Gi")
        )


@dataclass
class ProjectInfo:
    """Project information container."""
    name: str
    description: str
    owner: str
    team: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)
    spec: Dict[str, Any] = field(default_factory=dict)
    status: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkspaceInfo:
    """Workspace information container."""
    workspace_id: str
    project_ref: str
    name: str
    description: str
    requester: str
    environment: str
    resources: ResourceQuota
    approvers: List[str]
    phase: WorkspacePhase = WorkspacePhase.PENDING


class KubernetesClientError(Exception):
    """Custom exception for Kubernetes client errors."""
    pass


class ResourceManager(ABC):
    """Abstract base class for resource managers."""
    
    @abstractmethod
    def create(self, *args, **kwargs) -> bool:
        """Create the resource."""
        pass
    
    @abstractmethod
    def delete(self, *args, **kwargs) -> bool:
        """Delete the resource."""
        pass


class NamespaceManager(ResourceManager):
    """Manages Kubernetes namespace operations."""
    
    def __init__(self, v1_client: kubernetes.client.CoreV1Api):
        self.v1 = v1_client
    
    def create(self, workspace_name: str, workspace_info: WorkspaceInfo, 
               owner_ref: Dict[str, Any]) -> bool:
        """Create a Kubernetes namespace."""
        try:
            logger.info(f"[{workspace_name}] Creating namespace")
            
            namespace = kubernetes.client.V1Namespace(
                metadata=kubernetes.client.V1ObjectMeta(
                    name=workspace_name,
                    labels={
                        "app.kubernetes.io/managed-by": "xdew-operator",
                        "app.kubernetes.io/name": workspace_name,
                        "xdew.ch/project": workspace_info.project_ref,
                        "xdew.ch/environment": workspace_info.environment,
                        "xdew.ch/workspace": workspace_info.workspace_id
                    },
                    annotations={
                        "xdew.ch/requester": workspace_info.requester,
                        "xdew.ch/description": workspace_info.description
                    },
                    owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
                )
            )
            self.v1.create_namespace(namespace)
            logger.info(f"[{workspace_name}]   ↳ Namespace created successfully")
            return True
        except ApiException as e:
            logger.error(f"[{workspace_name}]   ↳ Failed to create namespace: {e}")
            return False
    
    def delete(self, workspace_name: str) -> bool:
        """Delete a Kubernetes namespace."""
        try:
            logger.info(f"[{workspace_name}] Deleting namespace")
            self.v1.delete_namespace(workspace_name)
            logger.info(f"[{workspace_name}]   ↳ Namespace deleted successfully")
            return True
        except ApiException as e:
            if e.status == 404:
                logger.info(f"[{workspace_name}]   ↳ Namespace already deleted")
                return True
            logger.error(f"[{workspace_name}]   ↳ Failed to delete namespace: {e}")
            return False


class ResourceQuotaManager(ResourceManager):
    """Manages Kubernetes resource quota operations."""
    
    def __init__(self, v1_client: kubernetes.client.CoreV1Api):
        self.v1 = v1_client
    
    def create(self, workspace_name: str, quota: ResourceQuota,
               owner_ref: Dict[str, Any]) -> bool:
        """Create a resource quota in the namespace."""
        try:
            logger.info(f"[{workspace_name}] Creating resource quota")
            
            resource_quota = kubernetes.client.V1ResourceQuota(
                metadata=kubernetes.client.V1ObjectMeta(
                    name="xdew-quota",
                    namespace=workspace_name,
                    owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
                ),
                spec=kubernetes.client.V1ResourceQuotaSpec(
                    hard={
                        "requests.cpu": quota.cpu,
                        "requests.memory": quota.memory,
                        "requests.storage": quota.storage,
                        "pods": "20"  # Default pod limit
                    }
                )
            )
            self.v1.create_namespaced_resource_quota(workspace_name, resource_quota)
            logger.info(f"[{workspace_name}]   ↳ Resource quota created successfully")
            return True
        except ApiException as e:
            logger.error(f"[{workspace_name}]   ↳ Failed to create resource quota: {e}")
            return False
    
    def update(self, workspace_name: str, quota: ResourceQuota) -> bool:
        """Update a resource quota in the namespace."""
        try:
            logger.info(f"[{workspace_name}] Updating resource quota")
            
            resource_quota = self.v1.read_namespaced_resource_quota(
                name="xdew-quota",
                namespace=workspace_name
            )
            
            resource_quota.spec.hard = {
                "requests.cpu": quota.cpu,
                "requests.memory": quota.memory,
                "requests.storage": quota.storage,
                "pods": "20"
            }
            
            self.v1.replace_namespaced_resource_quota(
                name="xdew-quota",
                namespace=workspace_name,
                body=resource_quota
            )
            logger.info(f"[{workspace_name}]   ↳ Resource quota updated successfully")
            return True
        except ApiException as e:
            logger.error(f"[{workspace_name}]   ↳ Failed to update resource quota: {e}")
            return False
    
    def delete(self, workspace_name: str) -> bool:
        """Delete resource quota (handled by namespace deletion)."""
        return True


class RBACManager(ResourceManager):
    """Manages RBAC resources (roles and role bindings)."""
    
    def __init__(self, rbac_client: kubernetes.client.RbacAuthorizationV1Api):
        self.rbac_v1 = rbac_client
    
    def create(self, workspace_name: str, workspace_info: WorkspaceInfo,
               project_info: ProjectInfo, owner_ref: Dict[str, Any]) -> bool:
        """Create RBAC resources for the namespace."""
        try:
            logger.info(f"[{workspace_name}] Creating RBAC resources")
            self._create_workspace_roles(workspace_name, workspace_info, owner_ref)
            self._create_team_bindings(workspace_name, project_info, owner_ref)
            logger.info(f"[{workspace_name}]   ↳ RBAC resources created successfully")
            return True
        except ApiException as e:
            logger.error(f"[{workspace_name}]   ↳ Failed to create RBAC resources: {e}")
            return False
    
    def delete(self, workspace_name: str) -> bool:
        """Delete RBAC resources (handled by namespace deletion)."""
        return True
    
    def _create_workspace_roles(self, workspace_name: str, workspace_info: WorkspaceInfo,
                               owner_ref: Dict[str, Any]) -> None:
        """Create workspace-specific roles."""
        # Admin role (for workspace requester and approvers)
        admin_role = kubernetes.client.V1Role(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{workspace_name}-admin",
                namespace=workspace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            rules=self._get_admin_policy_rules()
        )
        self.rbac_v1.create_namespaced_role(workspace_name, admin_role)
        
        # Developer role
        dev_role = kubernetes.client.V1Role(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{workspace_name}-developer",
                namespace=workspace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            rules=self._get_developer_policy_rules()
        )
        self.rbac_v1.create_namespaced_role(workspace_name, dev_role)
        
        # Readonly role
        readonly_role = kubernetes.client.V1Role(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{workspace_name}-readonly",
                namespace=workspace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            rules=self._get_readonly_policy_rules()
        )
        self.rbac_v1.create_namespaced_role(workspace_name, readonly_role)
        
        # Bind requester to admin role
        requester_binding = kubernetes.client.V1RoleBinding(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{workspace_name}-requester-admin",
                namespace=workspace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            subjects=[{
                "kind": "User",
                "name": workspace_info.requester,
                "api_group": "rbac.authorization.k8s.io"
            }],
            role_ref=kubernetes.client.V1RoleRef(
                kind="Role",
                name=f"{workspace_name}-admin",
                api_group="rbac.authorization.k8s.io"
            )
        )
        self.rbac_v1.create_namespaced_role_binding(workspace_name, requester_binding)
        
        # Bind approvers to admin role
        for approver_group in workspace_info.approvers:
            approver_binding = kubernetes.client.V1RoleBinding(
                metadata=kubernetes.client.V1ObjectMeta(
                    name=f"{workspace_name}-{approver_group}-admin",
                    namespace=workspace_name,
                    owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
                ),
                subjects=[{
                    "kind": "Group",
                    "name": approver_group,
                    "api_group": "rbac.authorization.k8s.io"
                }],
                role_ref=kubernetes.client.V1RoleRef(
                    kind="Role",
                    name=f"{workspace_name}-admin",
                    api_group="rbac.authorization.k8s.io"
                )
            )
            self.rbac_v1.create_namespaced_role_binding(workspace_name, approver_binding)
    
    def _create_team_bindings(self, workspace_name: str, project_info: ProjectInfo,
                             owner_ref: Dict[str, Any]) -> None:
        """Create team-based role bindings."""
        for team in project_info.team:
            # Determine role based on team name
            if "admin" in team.lower():
                role_name = f"{workspace_name}-admin"
            elif "readonly" in team.lower() or "read" in team.lower():
                role_name = f"{workspace_name}-readonly"
            else:
                role_name = f"{workspace_name}-developer"
            
            team_binding = kubernetes.client.V1RoleBinding(
                metadata=kubernetes.client.V1ObjectMeta(
                    name=f"{workspace_name}-{team}",
                    namespace=workspace_name,
                    owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
                ),
                subjects=[{
                    "kind": "Group",
                    "name": team,
                    "api_group": "rbac.authorization.k8s.io"
                }],
                role_ref=kubernetes.client.V1RoleRef(
                    kind="Role",
                    name=role_name,
                    api_group="rbac.authorization.k8s.io"
                )
            )
            self.rbac_v1.create_namespaced_role_binding(workspace_name, team_binding)
    
    @staticmethod
    def _get_admin_policy_rules() -> List[kubernetes.client.V1PolicyRule]:
        """Get policy rules for admin role."""
        return [
            kubernetes.client.V1PolicyRule(
                api_groups=["*"],
                resources=["*"],
                verbs=["*"]
            )
        ]
    
    @staticmethod
    def _get_developer_policy_rules() -> List[kubernetes.client.V1PolicyRule]:
        """Get policy rules for developer role."""
        return [
            kubernetes.client.V1PolicyRule(
                api_groups=[""],
                resources=["pods", "services", "configmaps", "secrets", "persistentvolumeclaims"],
                verbs=["get", "list", "watch", "create", "update", "patch", "delete"]
            ),
            kubernetes.client.V1PolicyRule(
                api_groups=["apps"],
                resources=["deployments", "replicasets", "statefulsets", "daemonsets"],
                verbs=["get", "list", "watch", "create", "update", "patch", "delete"]
            ),
            kubernetes.client.V1PolicyRule(
                api_groups=["networking.k8s.io"],
                resources=["ingresses", "networkpolicies"],
                verbs=["get", "list", "watch", "create", "update", "patch", "delete"]
            ),
            kubernetes.client.V1PolicyRule(
                api_groups=[""],
                resources=["events"],
                verbs=["get", "list", "watch"]
            )
        ]
    
    @staticmethod
    def _get_readonly_policy_rules() -> List[kubernetes.client.V1PolicyRule]:
        """Get policy rules for readonly role."""
        return [
            kubernetes.client.V1PolicyRule(
                api_groups=["", "apps", "extensions", "networking.k8s.io"],
                resources=["*"],
                verbs=["get", "list", "watch"]
            )
        ]


class XDEWOperator:
    """Main operator class for managing XDEW projects and workspaces."""
    
    def __init__(self):
        """Initialize the operator with Kubernetes clients."""
        self._initialize_kubernetes_clients()
        self._initialize_managers()
    
    def _initialize_kubernetes_clients(self) -> None:
        """Initialize Kubernetes API clients."""
        try:
            kubernetes.config.load_incluster_config()
            logger.info("Using in-cluster configuration")
        except kubernetes.config.ConfigException:
            try:
                kubernetes.config.load_kube_config()
                logger.info("Using local kubeconfig")
            except kubernetes.config.ConfigException as e:
                logger.error("Could not configure kubernetes client")
                raise KubernetesClientError("Failed to configure Kubernetes client") from e
        
        self.v1 = kubernetes.client.CoreV1Api()
        self.rbac_v1 = kubernetes.client.RbacAuthorizationV1Api()
        self.custom_api = kubernetes.client.CustomObjectsApi()
    
    def _initialize_managers(self) -> None:
        """Initialize resource managers."""
        self.namespace_manager = NamespaceManager(self.v1)
        self.quota_manager = ResourceQuotaManager(self.v1)
        self.rbac_manager = RBACManager(self.rbac_v1)
    
    def get_project_by_id(self, project_id: str) -> Optional[ProjectInfo]:
        """Retrieve project information by ID."""
        try:
            project = self.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="projects",
                name=project_id
            )
            
            spec = project.get("spec", {})
            return ProjectInfo(
                name=spec.get("name", ""),
                description=spec.get("description", ""),
                owner=spec.get("owner", ""),
                team=spec.get("team", []),
                metadata=project.get("metadata", {}),
                spec=spec,
                status=project.get("status", {})
            )
        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Project {project_id} not found")
                return None
            logger.error(f"Failed to get project {project_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting project {project_id}: {e}")
            return None
    
    def create_workspace_resources(self, workspace_info: WorkspaceInfo,
                                  project_info: ProjectInfo, owner_ref: Dict[str, Any]) -> bool:
        """Create all resources for a workspace."""
        workspace_name = workspace_info.name
        
        try:
            # Create namespace
            if not self.namespace_manager.create(workspace_name, workspace_info, owner_ref):
                return False
            
            # Create resource quota
            if not self.quota_manager.create(workspace_name, workspace_info.resources, owner_ref):
                return False
            
            # Create RBAC resources
            if not self.rbac_manager.create(workspace_name, workspace_info, project_info, owner_ref):
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"[{workspace_name}] Failed to create workspace resources: {e}")
            return False
    
    def update_workspace_resources(self, workspace_name: str, quota: ResourceQuota) -> bool:
        """Update workspace resources."""
        try:
            logger.info(f"[{workspace_name}] Updating workspace resources")
            return self.quota_manager.update(workspace_name, quota)
        except Exception as e:
            logger.error(f"[{workspace_name}] Failed to update workspace resources: {e}")
            return False
    
    def delete_workspace_resources(self, workspace_name: str) -> bool:
        """Delete all resources for a workspace."""
        return self.namespace_manager.delete(workspace_name)
    
    def add_audit_entry(self, name: str, resource_type: str, user: str, 
                       action: str, status: str, comment: str) -> None:
        """Add an audit entry to the resource."""
        try:
            resource = self.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural=resource_type,
                name=name
            )
            
            if "status" not in resource:
                resource["status"] = {}
            
            if "auditLog" not in resource["status"]:
                resource["status"]["auditLog"] = []
            
            # Create new audit entry
            entry = AuditEntry(
                timestamp=datetime.now(timezone.utc).isoformat(),
                user=user,
                action=action,
                status=status,
                comment=comment
            )
            
            # Add to audit log
            resource["status"]["auditLog"].append(entry.to_dict())
            
            # Update last updated timestamp
            resource["status"]["lastUpdated"] = entry.timestamp
            
            # Keep only last 100 entries
            if len(resource["status"]["auditLog"]) > 100:
                resource["status"]["auditLog"] = resource["status"]["auditLog"][-100:]
            
            self.custom_api.patch_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural=resource_type,
                name=name,
                body=resource
            )
            logger.info(f"[{name}] Audit entry added: {action} - {status}")
        except Exception as e:
            logger.error(f"[{name}] Failed to add audit entry: {e}")
    
    def update_project_workspace_count(self, project_id: str) -> None:
        """Update the workspace count for a project."""
        try:
            logger.info(f"[{project_id}] Updating workspace count")
            
            # Get all workspaces
            workspaces = self.custom_api.list_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="workspaces"
            )
            
            # Count workspaces for this project (excluding those being deleted)
            count = 0
            for ws in workspaces.get("items", []):
                # Skip workspaces that are being deleted (have deletionTimestamp)
                if ws.get("metadata", {}).get("deletionTimestamp"):
                    continue
                if ws.get("spec", {}).get("projectRef") == project_id:
                    count += 1
            
            # Update project status
            try:
                project = self.custom_api.get_cluster_custom_object(
                    group="xdew.ch",
                    version="v1",
                    plural="projects",
                    name=project_id
                )
                
                if "status" not in project:
                    project["status"] = {}
                
                project["status"]["workspaceCount"] = count
                project["status"]["lastUpdated"] = datetime.now(timezone.utc).isoformat()
                
                self.custom_api.patch_cluster_custom_object(
                    group="xdew.ch",
                    version="v1",
                    plural="projects",
                    name=project_id,
                    body=project
                )
                
                logger.info(f"[{project_id}]   ↳ Workspace count updated to {count}")
                
            except ApiException as api_error:
                if api_error.status == 404:
                    logger.warning(f"[{project_id}]   ↳ Project not found, skipping count update")
                else:
                    raise api_error
                
        except Exception as e:
            logger.error(f"[{project_id}] Failed to update workspace count: {e}")
    
    def cleanup_project_workspaces(self, project_id: str) -> None:
        """Clean up all workspaces associated with a project."""
        try:
            logger.info(f"[{project_id}] Cleaning up associated workspaces")
            
            workspaces = self.custom_api.list_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="workspaces"
            )
            
            count = 0
            for ws in workspaces.get("items", []):
                if ws.get("spec", {}).get("projectRef") == project_id:
                    ws_name = ws["metadata"]["name"]
                    logger.info(f"[{project_id}]   ↳ Deleting workspace: {ws_name}")
                    
                    try:
                        self.custom_api.delete_cluster_custom_object(
                            group="xdew.ch",
                            version="v1",
                            plural="workspaces",
                            name=ws_name
                        )
                        count += 1
                    except Exception as e:
                        logger.error(f"[{project_id}]   ↳ Failed to delete workspace {ws_name}: {e}")
            
            logger.info(f"[{project_id}]   ↳ Cleaned up {count} workspaces")
        except Exception as e:
            logger.error(f"[{project_id}] Failed to clean up workspaces: {e}")
                        
    def force_cleanup_stuck_workspaces(self) -> None:
        """Force cleanup of workspaces stuck in deletion."""
        try:
            logger.info("Checking for stuck workspaces in deletion")
            
            workspaces = self.custom_api.list_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="workspaces"
            )
            
            finalizer_name = 'xdew.ch/workspace-cleanup'
            
            for ws in workspaces.get("items", []):
                metadata = ws.get("metadata", {})
                deletion_timestamp = metadata.get("deletionTimestamp")
                finalizers = metadata.get("finalizers", [])
                name = metadata.get("name", "unknown")
                
                # Check if workspace is stuck in deletion with our finalizer
                if deletion_timestamp and finalizer_name in finalizers:
                    logger.info(f"[{name}] Found stuck workspace, forcing cleanup")
                    
                    try:
                        # Remove namespace directly
                        try:
                            self.v1.delete_namespace(name)
                            logger.info(f"[{name}]   ↳ Namespace deletion initiated")
                        except ApiException as e:
                            if e.status == 404:
                                logger.info(f"[{name}]   ↳ Namespace already deleted")
                            else:
                                logger.warning(f"[{name}]   ↳ Failed to delete namespace: {e}")
                        
                        # Update project count
                        project_ref = ws.get("spec", {}).get("projectRef")
                        if project_ref:
                            self.update_project_workspace_count(project_ref)
                        
                        # Remove finalizer to unblock deletion
                        new_finalizers = [f for f in finalizers if f != finalizer_name]
                        patch_body = {
                            "metadata": {
                                "finalizers": new_finalizers
                            }
                        }
                        
                        self.custom_api.patch_cluster_custom_object(
                            group="xdew.ch",
                            version="v1",
                            plural="workspaces",
                            name=name,
                            body=patch_body
                        )
                        
                        logger.info(f"[{name}]   ↳ Forced cleanup completed")
                        
                    except Exception as cleanup_error:
                        logger.error(f"[{name}]   ↳ Failed to force cleanup: {cleanup_error}")
                        
        except Exception as e:
            logger.error(f"Failed to check for stuck workspaces: {e}")


# Add a timer to periodically check for stuck workspaces
@kopf.timer('xdew.ch', 'v1', 'workspaces', interval=300)  # Every 5 minutes
def cleanup_stuck_workspaces_timer(**kwargs):
    """Timer to periodically clean up stuck workspaces."""
    try:
        operator.force_cleanup_stuck_workspaces()
    except Exception as e:
        logger.error(f"Failed to run stuck workspace cleanup timer: {e}")
operator = XDEWOperator()


@kopf.on.create('xdew.ch', 'v1', 'projects')
def create_project(spec, name, patch, **kwargs):
    """Handle project creation events."""
    logger.info(f"[{name}] Creating project")
    
    try:
        # Validate required fields
        project_name = spec.get('name')
        description = spec.get('description')
        owner = spec.get('owner')
        team = spec.get('team', [])
        
        if not all([project_name, description, owner, team]):
            message = "Missing required fields: name, description, owner, or team"
            logger.error(f"[{name}]   ↳ {message}")
            patch.status['phase'] = ProjectPhase.SUSPENDED.value
            patch.status['workspaceCount'] = 0
            patch.status['createdAt'] = datetime.now(timezone.utc).isoformat()
            patch.status['lastUpdated'] = datetime.now(timezone.utc).isoformat()
            patch.status['auditLog'] = [
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "user": "system",
                    "action": "project-creation-failed",
                    "status": "suspended",
                    "comment": message
                }
            ]
            return
        
        # Initialize status
        timestamp = datetime.now(timezone.utc).isoformat()
        patch.status['phase'] = ProjectPhase.ACTIVE.value
        patch.status['workspaceCount'] = 0
        patch.status['createdAt'] = timestamp
        patch.status['lastUpdated'] = timestamp
        patch.status['auditLog'] = [
            {
                "timestamp": timestamp,
                "user": owner,
                "action": "project-created",
                "status": "active",
                "comment": f"Project '{project_name}' created successfully"
            }
        ]
        
        logger.info(f"[{name}]   ↳ Project created successfully")
        
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to create project: {e}")
        timestamp = datetime.now(timezone.utc).isoformat()
        patch.status['phase'] = ProjectPhase.SUSPENDED.value
        patch.status['workspaceCount'] = 0
        patch.status['createdAt'] = timestamp
        patch.status['lastUpdated'] = timestamp
        patch.status['auditLog'] = [
            {
                "timestamp": timestamp,
                "user": "system",
                "action": "project-creation-failed",
                "status": "suspended",
                "comment": f"Creation failed: {str(e)}"
            }
        ]


@kopf.on.create('xdew.ch', 'v1', 'workspaces')
def create_workspace(spec, name, patch, uid, **kwargs):
    """Handle workspace creation events."""
    logger.info(f"[{name}] Creating workspace")
    
    try:
        # Extract workspace information
        project_ref = spec.get('projectRef')
        workspace_name = spec.get('name')
        description = spec.get('description')
        requester = spec.get('requester')
        environment = spec.get('environment')
        resources_spec = spec.get('resources', {})
        approvers = spec.get('approvers', [])
        
        # Validate required fields
        if not all([project_ref, workspace_name, description, requester, environment, approvers]):
            message = "Missing required fields"
            timestamp = datetime.now(timezone.utc).isoformat()
            patch.status['phase'] = WorkspacePhase.REJECTED.value
            patch.status['approvals'] = []
            patch.status['auditLog'] = [
                {
                    "timestamp": timestamp,
                    "user": "system",
                    "action": "workspace-creation-failed",
                    "status": "rejected",
                    "comment": message
                }
            ]
            patch.status['createdAt'] = timestamp
            logger.warning(f"[{name}]   ↳ {message}")
            return
        
        # Validate project exists
        project_info = operator.get_project_by_id(project_ref)
        if not project_info:
            message = f"Project {project_ref} not found"
            timestamp = datetime.now(timezone.utc).isoformat()
            patch.status['phase'] = WorkspacePhase.REJECTED.value
            patch.status['approvals'] = []
            patch.status['auditLog'] = [
                {
                    "timestamp": timestamp,
                    "user": "system",
                    "action": "workspace-creation-failed",
                    "status": "rejected",
                    "comment": message
                }
            ]
            patch.status['createdAt'] = timestamp
            logger.warning(f"[{name}]   ↳ {message}")
            return
        
        # Add finalizer to ensure proper cleanup
        if not hasattr(patch, 'metadata') or patch.metadata is None:
            patch.metadata = {}
        
        patch.metadata['finalizers'] = ['xdew.ch/workspace-cleanup']
        
        # Set owner reference to project
        project_ref_obj = {
            "apiVersion": "xdew.ch/v1",
            "kind": "Project",
            "name": project_ref,
            "uid": project_info.metadata.get("uid")
        }
        
        if project_ref_obj["uid"]:
            patch.metadata['ownerReferences'] = [project_ref_obj]
        
        # Set default resource quota if not provided
        if not resources_spec:
            resources_spec = ResourceQuota().to_dict()
            patch.spec['resources'] = resources_spec
            logger.info(f"[{name}]   ↳ Applied default resource quota")
        
        # Initialize status
        timestamp = datetime.now(timezone.utc).isoformat()
        patch.status['phase'] = WorkspacePhase.PENDING.value
        patch.status['approvals'] = []
        patch.status['auditLog'] = [
            {
                "timestamp": timestamp,
                "user": requester,
                "action": "workspace-created",
                "status": "pending",
                "comment": f"Workspace '{workspace_name}' creation requested for environment '{environment}'"
            }
        ]
        patch.status['createdAt'] = timestamp
        
        # Update project workspace count
        operator.update_project_workspace_count(project_ref)
        
        logger.info(f"[{name}] Workspace created in pending state")
        
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to create workspace: {e}")
        timestamp = datetime.now(timezone.utc).isoformat()
        patch.status['phase'] = WorkspacePhase.REJECTED.value
        patch.status['approvals'] = []
        patch.status['auditLog'] = [
            {
                "timestamp": timestamp,
                "user": "system",
                "action": "workspace-creation-failed",
                "status": "rejected",
                "comment": f"Creation failed: {str(e)}"
            }
        ]
        patch.status['createdAt'] = timestamp


@kopf.on.field('xdew.ch', 'v1', 'workspaces', field='status.phase')
def handle_workspace_phase_change(old, new, spec, name, uid, body, **kwargs):
    """Handle workspace phase changes."""
    logger.info(f"[{name}] Phase changed from {old} to {new}")
    
    # Check if this phase change was done manually (without audit entry)
    current_status = body.get("status", {})
    audit_log = current_status.get("auditLog", [])
    
    # If there's no recent audit entry for this phase, add one
    should_add_audit = True
    if audit_log:
        last_entry = audit_log[-1]
        if (last_entry.get("status") == new and 
            last_entry.get("timestamp") and
            _is_recent_timestamp(last_entry["timestamp"])):
            should_add_audit = False
    
    if should_add_audit:
        # Extract user from annotation or patch metadata if available
        user = _extract_user_from_patch(body)
        if not user:
            user = "system"
        
        message = f"Phase manually changed to {new}"
        if user != "system":
            message += f" by {user}"
        
        operator.add_audit_entry(name, "workspaces", user, "phase-changed", new, message)
    
    # Handle phase-specific logic
    if new == WorkspacePhase.APPROVED.value:
        _handle_workspace_approval(spec, name, uid, body)
    elif new == WorkspacePhase.ACTIVE.value:
        _handle_workspace_activation(spec, name, uid)
    elif new == WorkspacePhase.TERMINATED.value:
        _handle_workspace_termination(name)


@kopf.on.field('xdew.ch', 'v1', 'workspaces', field='status.approvals')
def handle_workspace_approvals_change(old, new, spec, name, body, **kwargs):
    """Handle workspace approvals changes."""
    if old != new and new:
        logger.info(f"[{name}] Approvals updated")
        
        # Check if we have enough approvals to move to approved state
        approvals = new or []
        approved_count = sum(1 for approval in approvals if approval.get("decision") == "approved")
        rejected_count = sum(1 for approval in approvals if approval.get("decision") == "rejected")
        
        current_phase = body.get("status", {}).get("phase")
        
        # Auto-approve if at least one approval and no rejections
        if approved_count > 0 and rejected_count == 0 and current_phase == WorkspacePhase.PENDING.value:
            logger.info(f"[{name}] Auto-approving workspace based on approvals")
            try:
                # Update phase to approved
                workspace = operator.custom_api.get_cluster_custom_object(
                    group="xdew.ch",
                    version="v1",
                    plural="workspaces",
                    name=name
                )
                
                workspace["status"]["phase"] = WorkspacePhase.APPROVED.value
                workspace["status"]["approvedAt"] = datetime.now(timezone.utc).isoformat()
                
                operator.custom_api.patch_cluster_custom_object(
                    group="xdew.ch",
                    version="v1",
                    plural="workspaces",
                    name=name,
                    body=workspace
                )
            except Exception as e:
                logger.error(f"[{name}] Failed to auto-approve workspace: {e}")
        
        # Auto-reject if any rejection
        elif rejected_count > 0 and current_phase == WorkspacePhase.PENDING.value:
            logger.info(f"[{name}] Auto-rejecting workspace based on rejections")
            try:
                workspace = operator.custom_api.get_cluster_custom_object(
                    group="xdew.ch",
                    version="v1",
                    plural="workspaces",
                    name=name
                )
                
                workspace["status"]["phase"] = WorkspacePhase.REJECTED.value
                
                operator.custom_api.patch_cluster_custom_object(
                    group="xdew.ch",
                    version="v1",
                    plural="workspaces",
                    name=name,
                    body=workspace
                )
            except Exception as e:
                logger.error(f"[{name}] Failed to auto-reject workspace: {e}")


@kopf.on.field('xdew.ch', 'v1', 'workspaces', field='spec.resources')
def handle_resource_quota_change(old, new, spec, name, **kwargs):
    """Handle resource quota changes."""
    if old != new and old is not None:  # Skip initial creation
        logger.info(f"[{name}] Resource quota changed")
        
        try:
            quota = ResourceQuota.from_dict(new)
            if operator.update_workspace_resources(name, quota):
                operator.add_audit_entry(
                    name, "workspaces", "system", "resource-quota-updated", 
                    "updated", "Resource quota updated successfully"
                )
            else:
                operator.add_audit_entry(
                    name, "workspaces", "system", "resource-quota-update-failed", 
                    "error", "Failed to update resource quota"
                )
        except Exception as e:
            logger.error(f"[{name}] Failed to handle quota change: {e}")
            operator.add_audit_entry(
                name, "workspaces", "system", "resource-quota-update-failed", 
                "error", f"Failed to update quota: {str(e)}"
            )


def _is_recent_timestamp(timestamp_str: str) -> bool:
    """Check if timestamp is within the last 10 seconds."""
    try:
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        return (now - timestamp).total_seconds() < 10
    except:
        return False


def _extract_user_from_patch(body: Dict[str, Any]) -> Optional[str]:
    """Extract user information from resource annotations or managed fields."""
    try:
        # Check for user annotation
        annotations = body.get("metadata", {}).get("annotations", {})
        user = annotations.get("xdew.ch/last-modified-by")
        if user:
            return user
        
        # Try to extract from managed fields (last kubectl user)
        managed_fields = body.get("metadata", {}).get("managedFields", [])
        for field in reversed(managed_fields):  # Most recent first
            if field.get("operation") == "Update" and field.get("manager"):
                manager = field.get("manager", "")
                if "kubectl" in manager:
                    return "kubectl-user"  # Default for kubectl operations
        
        return None
    except:
        return None


def _handle_workspace_approval(spec, name, uid, body):
    """Handle workspace approval logic."""
    try:
        logger.info(f"[{name}] Processing workspace approval")
        
        # Add audit entry for approval
        operator.add_audit_entry(
            name, "workspaces", "system", "workspace-approved", 
            "approved", "Workspace approved for resource creation"
        )
        
        # Automatically transition to active state
        logger.info(f"[{name}] Auto-activating approved workspace")
        workspace = operator.custom_api.get_cluster_custom_object(
            group="xdew.ch",
            version="v1",
            plural="workspaces",
            name=name
        )
        
        workspace["status"]["phase"] = WorkspacePhase.ACTIVE.value
        
        operator.custom_api.patch_cluster_custom_object(
            group="xdew.ch",
            version="v1",
            plural="workspaces",
            name=name,
            body=workspace
        )
        
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to handle workspace approval: {e}")
        operator.add_audit_entry(
            name, "workspaces", "system", "workspace-approval-failed", 
            "error", f"Failed to process approval: {str(e)}"
        )


def _handle_workspace_activation(spec, name, uid):
    """Handle workspace activation logic."""
    try:
        logger.info(f"[{name}] Processing workspace activation")
        
        # Extract workspace information
        project_ref = spec.get('projectRef')
        workspace_name = spec.get('name')
        description = spec.get('description')
        requester = spec.get('requester')
        environment = spec.get('environment')
        resources_spec = spec.get('resources', {})
        approvers = spec.get('approvers', [])
        
        # Get project information
        project_info = operator.get_project_by_id(project_ref)
        if not project_info:
            operator.add_audit_entry(
                name, "workspaces", "system", "workspace-activation-failed", 
                "error", f"Project {project_ref} not found"
            )
            return
        
        # Create workspace info
        workspace_info = WorkspaceInfo(
            workspace_id=name,
            project_ref=project_ref,
            name=workspace_name,
            description=description,
            requester=requester,
            environment=environment,
            resources=ResourceQuota.from_dict(resources_spec),
            approvers=approvers,
            phase=WorkspacePhase.ACTIVE
        )
        
        # Create owner reference
        owner_ref = {
            "api_version": "xdew.ch/v1",
            "kind": "Workspace",
            "name": name,
            "uid": uid
        }
        
        # Create workspace resources
        if operator.create_workspace_resources(workspace_info, project_info, owner_ref):
            # Update namespace reference
            try:
                workspace = operator.custom_api.get_cluster_custom_object(
                    group="xdew.ch",
                    version="v1",
                    plural="workspaces",
                    name=name
                )
                
                workspace["status"]["namespaceRef"] = workspace_name
                
                operator.custom_api.patch_cluster_custom_object(
                    group="xdew.ch",
                    version="v1",
                    plural="workspaces",
                    name=name,
                    body=workspace
                )
            except Exception as e:
                logger.error(f"[{name}] Failed to update namespace reference: {e}")
            
            operator.add_audit_entry(
                name, "workspaces", "system", "namespace-created", 
                "active", f"Namespace '{workspace_name}' created and configured successfully"
            )
        else:
            operator.add_audit_entry(
                name, "workspaces", "system", "workspace-activation-failed", 
                "error", "Failed to create workspace resources"
            )
        
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to handle workspace activation: {e}")
        operator.add_audit_entry(
            name, "workspaces", "system", "workspace-activation-failed", 
            "error", f"Failed to activate workspace: {str(e)}"
        )


def _handle_workspace_termination(name):
    """Handle workspace termination logic."""
    try:
        logger.info(f"[{name}] Processing workspace termination")
        
        # Delete workspace resources
        if operator.delete_workspace_resources(name):
            operator.add_audit_entry(
                name, "workspaces", "system", "workspace-terminated", 
                "terminated", "Workspace resources deleted successfully"
            )
        else:
            operator.add_audit_entry(
                name, "workspaces", "system", "workspace-termination-failed", 
                "error", "Failed to delete workspace resources"
            )
            
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to terminate workspace: {e}")
        operator.add_audit_entry(
            name, "workspaces", "system", "workspace-termination-failed", 
            "error", f"Failed to terminate workspace: {str(e)}"
        )


@kopf.on.delete('xdew.ch', 'v1', 'workspaces')
def delete_workspace(spec, name, body, **kwargs):
    """Handle workspace deletion events."""
    logger.info(f"[{name}] Deleting workspace")
    
    try:
        project_ref = spec.get('projectRef')
        
        # Delete workspace resources first
        operator.delete_workspace_resources(name)
        
        # Update project workspace count AFTER deletion
        if project_ref:
            try:
                # Update count in a separate try block to ensure it happens
                operator.update_project_workspace_count(project_ref)
                logger.info(f"[{name}]   ↳ Updated workspace count for project {project_ref}")
            except Exception as count_error:
                logger.error(f"[{name}]   ↳ Failed to update project workspace count: {count_error}")
        
        logger.info(f"[{name}]   ↳ Workspace deletion completed")
            
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to handle workspace deletion: {e}")


# Alternative approach using finalizer handler
@kopf.on.delete('xdew.ch', 'v1', 'workspaces', optional=True)
def cleanup_workspace_finalizer(spec, name, body, patch, **kwargs):
    """Handle workspace cleanup with finalizer - alternative approach."""
    finalizer_name = 'xdew.ch/workspace-cleanup'
    current_finalizers = body.get("metadata", {}).get("finalizers", [])
    
    # Only process if our finalizer is present
    if finalizer_name not in current_finalizers:
        return
    
    logger.info(f"[{name}] Processing workspace cleanup with finalizer")
    
    try:
        project_ref = spec.get('projectRef')
        
        # Delete workspace resources
        operator.delete_workspace_resources(name)
        
        # Update project workspace count
        if project_ref:
            operator.update_project_workspace_count(project_ref)
            logger.info(f"[{name}]   ↳ Updated workspace count for project {project_ref}")
        
        # Remove our finalizer to allow deletion
        new_finalizers = [f for f in current_finalizers if f != finalizer_name]
        
        # Use kopf patch mechanism to remove finalizer
        patch.metadata = patch.metadata or {}
        patch.metadata['finalizers'] = new_finalizers
        
        logger.info(f"[{name}]   ↳ Workspace cleanup completed, finalizer removed")
            
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to cleanup workspace: {e}")
        # Let the deletion continue even if cleanup failed partially
        # Remove finalizer to prevent blocking
        try:
            new_finalizers = [f for f in current_finalizers if f != finalizer_name]
            patch.metadata = patch.metadata or {}
            patch.metadata['finalizers'] = new_finalizers
            logger.warning(f"[{name}]   ↳ Removed finalizer despite cleanup failure to prevent blocking")
        except Exception as patch_error:
            logger.error(f"[{name}]   ↳ Failed to remove finalizer: {patch_error}")
            # As last resort, raise temporary error to retry
            raise kopf.TemporaryError(f"Cleanup and finalizer removal failed: {e}", delay=30)


@kopf.on.delete('xdew.ch', 'v1', 'projects')
def delete_project(name, **kwargs):
    """Handle project deletion events."""
    logger.info(f"[{name}] Deleting project")
    
    try:
        # Add deletion audit entry
        operator.add_audit_entry(
            name, "projects", "system", "project-deleted", 
            "deleted", "Project deletion initiated"
        )
        
        # Clean up associated workspaces
        operator.cleanup_project_workspaces(name)
        
        operator.add_audit_entry(
            name, "projects", "system", "project-cleanup-completed", 
            "deleted", "Project and associated workspaces deleted successfully"
        )
        
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to handle project deletion: {e}")


def main():
    """Main entry point for the operator."""
    logger.info("Starting XDEW Kubernetes Operator v2")
    kopf.configure(verbose=True)
    kopf.run()


if __name__ == "__main__":
    main()
