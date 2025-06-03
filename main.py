#!/usr/bin/env python3
"""
XDEW Kubernetes Operator

A Kubernetes operator for managing XDEW projects and namespaces with
automatic resource provisioning, RBAC, and lifecycle management.
"""

import hashlib
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


class NamespaceState(Enum):
    """Enumeration of possible namespace states."""
    REQUESTED = "requested"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    DELETED = "deleted"


@dataclass
class StatusEntry:
    """Status history entry."""
    timestamp: str
    state: str
    message: str
    user: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        entry = {
            "timestamp": self.timestamp,
            "state": self.state,
            "message": self.message
        }
        if self.user:
            entry["user"] = self.user
        return entry
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StatusEntry':
        return cls(
            timestamp=data["timestamp"],
            state=data["state"],
            message=data["message"],
            user=data.get("user")
        )


@dataclass
class ResourceQuota:
    """Resource quota configuration."""
    cpu: str = "2"
    memory: str = "4Gi"
    storage: str = "10Gi"
    pods: str = "10"
    
    def to_dict(self) -> Dict[str, str]:
        return {
            "cpu": self.cpu,
            "memory": self.memory,
            "storage": self.storage,
            "pods": self.pods
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'ResourceQuota':
        return cls(
            cpu=data.get("cpu", "2"),
            memory=data.get("memory", "4Gi"),
            storage=data.get("storage", "10Gi"),
            pods=data.get("pods", "10")
        )


@dataclass
class ProjectInfo:
    """Project information container."""
    name: str
    user_id: str
    description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    spec: Dict[str, Any] = field(default_factory=dict)
    status: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NamespaceInfo:
    """Namespace information container."""
    namespace_id: str
    display_name: str
    project_id: str
    description: str = ""
    resource_quota: Optional[ResourceQuota] = None
    state: NamespaceState = NamespaceState.REQUESTED


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
    
    def create(self, workspace_name: str, project_id: str, 
               owner_ref: Dict[str, Any], project_info: ProjectInfo) -> bool:
        """Create a Kubernetes namespace."""
        try:
            logger.info(f"[{workspace_name}] Creating namespace")
            
            namespace = kubernetes.client.V1Namespace(
                metadata=kubernetes.client.V1ObjectMeta(
                    name=workspace_name,
                    labels={
                        "app.kubernetes.io/managed-by": "xdew-operator",
                        "app.kubernetes.io/name": workspace_name,
                    },
                    owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
                )
            )
            self.v1.create_namespace(namespace)
            logger.info(f"[{workspace_name}]   ↳ Namespace created successfully")
            return True
        except ApiException as e:
            logger.error(f"[{workspace_name}]   ↳ Failed to create namespace: {e}")
            return Falsexdew
    
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
                        "pods": quota.pods
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
                "pods": quota.pods
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
    
    def create(self, workspace_name: str, project_id: str,
               owner_ref: Dict[str, Any]) -> bool:
        """Create RBAC resources for the namespace."""
        try:
            logger.info(f"[{workspace_name}] Creating RBAC resources")
            self._create_access_role(workspace_name, project_id, owner_ref)
            self._create_readonly_role(workspace_name, project_id, owner_ref)
            logger.info(f"[{workspace_name}]   ↳ RBAC resources created successfully")
            return True
        except ApiException as e:
            logger.error(f"[{workspace_name}]   ↳ Failed to create RBAC resources: {e}")
            return False
    
    def delete(self, workspace_name: str) -> bool:
        """Delete RBAC resources (handled by namespace deletion)."""
        return True
    
    def _create_access_role(self, workspace_name: str, project_id: str,
                           owner_ref: Dict[str, Any]) -> None:
        """Create access role and binding."""
        role_name = f"{project_id}-access"
        
        logger.info(f"[{workspace_name}]   ↳ Creating access role: {role_name}")
        
        # Create access role
        role = kubernetes.client.V1Role(
            metadata=kubernetes.client.V1ObjectMeta(
                name=role_name,
                namespace=workspace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            rules=self._get_access_policy_rules()
        )
        self.rbac_v1.create_namespaced_role(workspace_name, role)
        
        # Create access role binding
        role_binding = kubernetes.client.V1RoleBinding(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{role_name}-binding",
                namespace=workspace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            subjects=[
                {
                    "kind": "Group",
                    "name": f"project-{project_id}-access",
                    "api_group": "rbac.authorization.k8s.io"
                }
            ],
            role_ref=kubernetes.client.V1RoleRef(
                kind="Role",
                name=role_name,
                api_group="rbac.authorization.k8s.io"
            )
        )
        self.rbac_v1.create_namespaced_role_binding(workspace_name, role_binding)
    
    def _create_readonly_role(self, workspace_name: str, project_id: str,
                             owner_ref: Dict[str, Any]) -> None:
        """Create readonly role and binding."""
        role_name = f"{project_id}-readonly"
        
        logger.info(f"[{workspace_name}]   ↳ Creating readonly role: {role_name}")
        
        # Create readonly role
        role = kubernetes.client.V1Role(
            metadata=kubernetes.client.V1ObjectMeta(
                name=role_name,
                namespace=workspace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            rules=self._get_readonly_policy_rules()
        )
        self.rbac_v1.create_namespaced_role(workspace_name, role)
        
        # Create readonly role binding
        role_binding = kubernetes.client.V1RoleBinding(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{role_name}-binding",
                namespace=workspace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            subjects=[
                {
                    "kind": "Group",
                    "name": f"project-{project_id}-readonly",
                    "api_group": "rbac.authorization.k8s.io"
                }
            ],
            role_ref=kubernetes.client.V1RoleRef(
                kind="Role",
                name=role_name,
                api_group="rbac.authorization.k8s.io"
            )
        )
        self.rbac_v1.create_namespaced_role_binding(workspace_name, role_binding)
    
    @staticmethod
    def _get_access_policy_rules() -> List[kubernetes.client.V1PolicyRule]:
        """Get policy rules for access role."""
        return [
            kubernetes.client.V1PolicyRule(
                api_groups=[""],
                resources=["*"],
                verbs=["*"]
            ),
            kubernetes.client.V1PolicyRule(
                api_groups=["apps"],
                resources=["*"],
                verbs=["*"]
            ),
            kubernetes.client.V1PolicyRule(
                api_groups=["extensions"],
                resources=["*"],
                verbs=["*"]
            ),
            kubernetes.client.V1PolicyRule(
                api_groups=["networking.k8s.io"],
                resources=["*"],
                verbs=["*"]
            )
        ]
    
    @staticmethod
    def _get_readonly_policy_rules() -> List[kubernetes.client.V1PolicyRule]:
        """Get policy rules for readonly role."""
        return [
            kubernetes.client.V1PolicyRule(
                api_groups=[""],
                resources=["*"],
                verbs=["get", "list", "watch"]
            ),
            kubernetes.client.V1PolicyRule(
                api_groups=["apps"],
                resources=["*"],
                verbs=["get", "list", "watch"]
            ),
            kubernetes.client.V1PolicyRule(
                api_groups=["extensions"],
                resources=["*"],
                verbs=["get", "list", "watch"]
            ),
            kubernetes.client.V1PolicyRule(
                api_groups=["networking.k8s.io"],
                resources=["*"],
                verbs=["get", "list", "watch"]
            )
        ]


class XDEWOperator:
    """Main operator class for managing XDEW projects and namespaces."""
    
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
    
    @staticmethod
    def get_default_resource_quota() -> ResourceQuota:
        """Get default resource quota configuration."""
        return ResourceQuota()
    
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
                user_id=spec.get("userId", ""),
                description=spec.get("description", ""),
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
    
    def create_workspace_resources(self, namespace_info: NamespaceInfo,
                                  project_info: ProjectInfo, owner_ref: Dict[str, Any]) -> bool:
        """Create all resources for a workspace."""
        workspace_name = namespace_info.namespace_id
        
        try:
            # Create namespace
            if not self.namespace_manager.create(
                workspace_name, namespace_info.project_id, owner_ref, project_info
            ):
                return False
            
            # Create resource quota
            quota = namespace_info.resource_quota or self.get_default_resource_quota()
            if not self.quota_manager.create(workspace_name, quota, owner_ref):
                return False
            
            # Create RBAC resources
            if not self.rbac_manager.create(
                workspace_name, namespace_info.project_id, owner_ref
            ):
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
    
    def add_status_entry(self, name: str, resource_type: str, state: str, 
                        message: str, user: Optional[str] = None) -> None:
        """Add a status entry to the resource."""
        try:
            resource = self.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural=resource_type,
                name=name
            )
            
            if "status" not in resource:
                resource["status"] = {}
            
            if "history" not in resource["status"]:
                resource["status"]["history"] = []
            
            # Create new status entry
            entry = StatusEntry(
                timestamp=datetime.now(timezone.utc).isoformat(),
                state=state,
                message=message,
                user=user
            )
            
            # Add to history
            resource["status"]["history"].append(entry.to_dict())
            
            # Update current status
            resource["status"]["currentState"] = state
            resource["status"]["lastUpdated"] = entry.timestamp
            
            # Keep only last 50 entries
            if len(resource["status"]["history"]) > 50:
                resource["status"]["history"] = resource["status"]["history"][-50:]
            
            self.custom_api.patch_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural=resource_type,
                name=name,
                body=resource
            )
            logger.info(f"[{name}] Status updated: {state} - {message}")
        except Exception as e:
            logger.error(f"[{name}] Failed to update status: {e}")
    
    def update_project_namespace_count(self, project_id: str) -> None:
        """Update the namespace count for a project."""
        try:
            logger.info(f"[{project_id}] Updating namespace count")
            
            project_info = self.get_project_by_id(project_id)
            if not project_info:
                return
            
            workspaces = self.custom_api.list_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="workspaces"
            )
            
            count = sum(
                1 for ws in workspaces.get("items", [])
                if ws.get("spec", {}).get("projectId") == project_id
            )
            
            project = self.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="projects",
                name=project_id
            )
            
            if "status" not in project:
                project["status"] = {}
            
            project["status"]["namespacesCount"] = count
            project["status"]["lastUpdated"] = datetime.now(timezone.utc).isoformat()
            
            self.custom_api.patch_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="projects",
                name=project_id,
                body=project
            )
            
            logger.info(f"[{project_id}]   ↳ Namespace count updated to {count}")
            
        except Exception as e:
            logger.error(f"[{project_id}] Failed to update namespace count: {e}")
    
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
                if ws.get("spec", {}).get("projectId") == project_id:
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
            logger.error(f"[{project_id}] Failed to cleanup workspaces: {e}")


# Global operator instance
operator = XDEWOperator()


@kopf.on.create('xdew.ch', 'v1', 'projects')
def create_project(spec, name, patch, **kwargs):
    """Handle project creation events."""
    logger.info(f"[{name}] Creating project")
    
    try:
        # Validate required fields
        project_name = spec.get('name')
        user_id = spec.get('userId')
        
        if not project_name or not user_id:
            message = "Missing required fields: name or userId"
            operator.add_status_entry(name, "projects", "rejected", message)
            logger.error(f"[{name}]   ↳ {message}")
            return
        
        # Initialize status
        patch.status = {
            "currentState": "created",
            "lastUpdated": datetime.now(timezone.utc).isoformat(),
            "namespacesCount": 0,
            "history": [
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "state": "created",
                    "message": f"Project '{project_name}' created by {user_id}"
                }
            ]
        }
        
        logger.info(f"[{name}]   ↳ Project created successfully")
        
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to create project: {e}")
        operator.add_status_entry(name, "projects", "error", f"Creation failed: {str(e)}")


@kopf.on.create('xdew.ch', 'v1', 'workspaces')
def create_workspace(spec, name, patch, uid, **kwargs):
    """Handle workspace creation events."""
    logger.info(f"[{name}] Creating workspace")
    
    try:
        # Extract workspace information
        display_name = spec.get('displayName', spec.get('name', ''))
        project_id = spec.get('projectId')
        description = spec.get('description', '')
        resource_quota_spec = spec.get('resourceQuota', {})
        
        # Validate project exists
        project_info = operator.get_project_by_id(project_id)
        if not project_info:
            message = f"Project {project_id} not found"
            patch.status = {
                "currentState": NamespaceState.REJECTED.value,
                "lastUpdated": datetime.now(timezone.utc).isoformat(),
                "history": [
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "state": NamespaceState.REJECTED.value,
                        "message": message
                    }
                ]
            }
            logger.warning(f"[{name}]   ↳ {message}")
            return
        
        # Set owner reference to project
        project_ref = {
            "api_version": "xdew.ch/v1",
            "kind": "Project",
            "name": project_id,
            "uid": project_info.metadata.get("uid")
        }
        
        if project_ref["uid"]:
            patch.metadata = patch.metadata or {}
            patch.metadata["ownerReferences"] = [project_ref]
        
        # Set default resource quota if not provided
        if not resource_quota_spec:
            resource_quota_spec = operator.get_default_resource_quota().to_dict()
            patch.spec['resourceQuota'] = resource_quota_spec
            logger.info(f"[{name}]   ↳ Applied default resource quota")
        
        # Initialize status
        patch.status = {
            "currentState": NamespaceState.REQUESTED.value,
            "lastUpdated": datetime.now(timezone.utc).isoformat(),
            "history": [
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "state": NamespaceState.REQUESTED.value,
                    "message": f"Workspace '{display_name}' creation requested"
                }
            ]
        }
        
        # Update project namespace count
        operator.update_project_namespace_count(project_id)
        
        logger.info(f"[{name}]   ↳ Workspace created in requested state")
        
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to create workspace: {e}")
        patch.status = {
            "currentState": NamespaceState.REJECTED.value,
            "lastUpdated": datetime.now(timezone.utc).isoformat(),
            "history": [
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "state": NamespaceState.REJECTED.value,
                    "message": f"Creation failed: {str(e)}"
                }
            ]
        }


@kopf.on.field('xdew.ch', 'v1', 'workspaces', field='status.currentState')
def handle_workspace_state_change(old, new, spec, name, uid, body, **kwargs):
    """Handle workspace state changes."""
    logger.info(f"[{name}] State changed from {old} to {new}")
    
    # Check if this state change was done manually (without history entry)
    current_status = body.get("status", {})
    history = current_status.get("history", [])
    
    # If there's no recent history entry for this state, add one
    should_add_history = True
    if history:
        last_entry = history[-1]
        if (last_entry.get("state") == new and 
            last_entry.get("timestamp") and
            # Check if timestamp is very recent (within last 10 seconds)
            _is_recent_timestamp(last_entry["timestamp"])):
            should_add_history = False
    
    if should_add_history:
        # Extract user from annotation or patch metadata if available
        user = _extract_user_from_patch(body)
        message = f"State manually changed to {new}"
        if user:
            message += f" by {user}"
        
        operator.add_status_entry(name, "workspaces", new, message, user)
    
    # Handle state-specific logic
    if new == NamespaceState.ACCEPTED.value:
        _handle_workspace_acceptance(spec, name, uid)
    elif new == NamespaceState.DELETED.value:
        _handle_workspace_deletion(name)


def _is_recent_timestamp(timestamp_str: str) -> bool:
    """Check if timestamp is within the last 10 seconds."""
    try:
        from datetime import datetime, timezone
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
                    # Try to extract user from kubectl manager string
                    # Format is usually "kubectl-patch" or "kubectl-edit"
                    return "kubectl-user"  # Default for kubectl operations
        
        return None
    except:
        return None


@kopf.on.field('xdew.ch', 'v1', 'workspaces', field='spec.resourceQuota')
def handle_resource_quota_change(old, new, spec, name, **kwargs):
    """Handle resource quota changes."""
    if old != new and old is not None:  # Skip initial creation
        logger.info(f"[{name}] Resource quota changed")
        
        try:
            quota = ResourceQuota.from_dict(new)
            if operator.update_workspace_resources(name, quota):
                operator.add_status_entry(
                    name, "workspaces", "updated", 
                    "Resource quota updated successfully"
                )
            else:
                operator.add_status_entry(
                    name, "workspaces", "error", 
                    "Failed to update resource quota"
                )
        except Exception as e:
            logger.error(f"[{name}] Failed to handle quota change: {e}")
            operator.add_status_entry(
                name, "workspaces", "error", 
                f"Failed to update quota: {str(e)}"
            )


def _handle_workspace_acceptance(spec, name, uid):
    """Handle workspace acceptance logic."""
    try:
        logger.info(f"[{name}] Processing workspace acceptance")
        
        # Extract workspace information
        display_name = spec.get('displayName', spec.get('name', ''))
        project_id = spec.get('projectId')
        resource_quota_spec = spec.get('resourceQuota', {})
        
        # Get project information
        project_info = operator.get_project_by_id(project_id)
        if not project_info:
            operator.add_status_entry(
                name, "workspaces", NamespaceState.REJECTED.value,
                f"Project {project_id} not found"
            )
            return
        
        # Create workspace info
        namespace_info = NamespaceInfo(
            namespace_id=name,
            display_name=display_name,
            project_id=project_id,
            resource_quota=ResourceQuota.from_dict(resource_quota_spec)
        )
        
        # Create owner reference
        owner_ref = {
            "api_version": "xdew.ch/v1",
            "kind": "Workspace",
            "name": name,
            "uid": uid
        }
        
        # Create workspace resources
        if operator.create_workspace_resources(namespace_info, project_info, owner_ref):
            operator.add_status_entry(
                name, "workspaces", NamespaceState.ACCEPTED.value,
                "Workspace accepted and resources created successfully"
            )
        else:
            operator.add_status_entry(
                name, "workspaces", NamespaceState.REJECTED.value,
                "Failed to create workspace resources"
            )
        
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to handle workspace acceptance: {e}")
        operator.add_status_entry(
            name, "workspaces", NamespaceState.REJECTED.value,
            f"Failed to create resources: {str(e)}"
        )


def _handle_workspace_deletion(name):
    """Handle workspace deletion logic."""
    try:
        logger.info(f"[{name}] Processing workspace deletion")
        
        workspace = operator.custom_api.get_cluster_custom_object(
            group="xdew.ch",
            version="v1",
            plural="workspaces",
            name=name
        )
        
        # The namespace name is the same as workspace name
        if operator.delete_workspace_resources(name):
            operator.add_status_entry(
                name, "workspaces", NamespaceState.DELETED.value,
                "Workspace resources deleted successfully"
            )
        else:
            operator.add_status_entry(
                name, "workspaces", "error",
                "Failed to delete workspace resources"
            )
            
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to delete workspace resources: {e}")


@kopf.on.delete('xdew.ch', 'v1', 'workspaces')
def delete_workspace(spec, name, patch, **kwargs):
    """Handle workspace deletion events."""
    logger.info(f"[{name}] Deleting workspace")
    
    try:
        project_id = spec.get('projectId')
        
        # Add deletion status entry
        operator.add_status_entry(
            name, "workspaces", NamespaceState.DELETED.value,
            "Workspace deletion initiated"
        )
        
        # Update project namespace count
        if project_id:
            operator.update_project_namespace_count(project_id)
            
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to handle workspace deletion: {e}")


@kopf.on.delete('xdew.ch', 'v1', 'projects')
def delete_project(name, **kwargs):
    """Handle project deletion events."""
    logger.info(f"[{name}] Deleting project")
    
    try:
        # Add deletion status entry
        operator.add_status_entry(
            name, "projects", "deleted",
            "Project deletion initiated"
        )
        
        # Clean up associated workspaces
        operator.cleanup_project_workspaces(name)
        
        operator.add_status_entry(
            name, "projects", "deleted",
            "Project and associated workspaces deleted successfully"
        )
        
    except Exception as e:
        logger.error(f"[{name}]   ↳ Failed to handle project deletion: {e}")


def main():
    """Main entry point for the operator."""
    logger.info("Starting XDEW Kubernetes Operator")
    kopf.configure(verbose=True)
    kopf.run()


if __name__ == "__main__":
    main()
