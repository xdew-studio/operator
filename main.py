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
    kubernetes_namespace: Optional[str] = None
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
    
    def create(self, k8s_namespace_name: str, project_id: str, 
               namespace_name: str, owner_ref: Dict[str, Any],
               project_info: ProjectInfo) -> bool:
        """Create a Kubernetes namespace."""
        try:
            namespace = kubernetes.client.V1Namespace(
                metadata=kubernetes.client.V1ObjectMeta(
                    name=k8s_namespace_name,
                    labels={
                        "xdew.ch/project-id": project_id,
                        "xdew.ch/namespace-name": namespace_name,
                        "xdew.ch/project-owner": project_info.user_id,
                        "managed-by": "xdew-operator"
                    },
                    owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
                )
            )
            self.v1.create_namespace(namespace)
            logger.info(f"Created Kubernetes namespace: {k8s_namespace_name}")
            return True
        except ApiException as e:
            logger.error(f"Failed to create namespace {k8s_namespace_name}: {e}")
            return False
    
    def delete(self, k8s_namespace_name: str) -> bool:
        """Delete a Kubernetes namespace."""
        try:
            self.v1.delete_namespace(k8s_namespace_name)
            logger.info(f"Deleted Kubernetes namespace: {k8s_namespace_name}")
            return True
        except ApiException as e:
            if e.status == 404:
                logger.info(f"Namespace {k8s_namespace_name} already deleted")
                return True
            logger.error(f"Failed to delete namespace {k8s_namespace_name}: {e}")
            return False


class ResourceQuotaManager(ResourceManager):
    """Manages Kubernetes resource quota operations."""
    
    def __init__(self, v1_client: kubernetes.client.CoreV1Api):
        self.v1 = v1_client
    
    def create(self, k8s_namespace_name: str, quota: ResourceQuota,
               owner_ref: Dict[str, Any]) -> bool:
        """Create a resource quota in the namespace."""
        try:
            resource_quota = kubernetes.client.V1ResourceQuota(
                metadata=kubernetes.client.V1ObjectMeta(
                    name="xdew-quota",
                    namespace=k8s_namespace_name,
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
            self.v1.create_namespaced_resource_quota(k8s_namespace_name, resource_quota)
            logger.info(f"Created resource quota in namespace: {k8s_namespace_name}")
            return True
        except ApiException as e:
            logger.error(f"Failed to create resource quota in {k8s_namespace_name}: {e}")
            return False
    
    def delete(self, k8s_namespace_name: str) -> bool:
        """Delete resource quota (handled by namespace deletion)."""
        return True


class RBACManager(ResourceManager):
    """Manages RBAC resources (roles and role bindings)."""
    
    def __init__(self, rbac_client: kubernetes.client.RbacAuthorizationV1Api):
        self.rbac_v1 = rbac_client
    
    def create(self, k8s_namespace_name: str, project_id: str,
               owner_ref: Dict[str, Any]) -> bool:
        """Create RBAC resources for the namespace."""
        try:
            self._create_admin_role(k8s_namespace_name, project_id, owner_ref)
            self._create_readonly_role(k8s_namespace_name, project_id, owner_ref)
            logger.info(f"Created RBAC resources for project: {project_id}")
            return True
        except ApiException as e:
            logger.error(f"Failed to create RBAC resources: {e}")
            return False
    
    def delete(self, k8s_namespace_name: str) -> bool:
        """Delete RBAC resources (handled by namespace deletion)."""
        return True
    
    def _create_admin_role(self, k8s_namespace_name: str, project_id: str,
                          owner_ref: Dict[str, Any]) -> None:
        """Create admin role and binding."""
        role_name = f"xdew-{project_id}-admin"
        
        # Create admin role
        role = kubernetes.client.V1Role(
            metadata=kubernetes.client.V1ObjectMeta(
                name=role_name,
                namespace=k8s_namespace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            rules=self._get_admin_policy_rules()
        )
        self.rbac_v1.create_namespaced_role(k8s_namespace_name, role)
        
        # Create admin role binding
        role_binding = kubernetes.client.V1RoleBinding(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{role_name}-binding",
                namespace=k8s_namespace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            subjects=[
                kubernetes.client.V1Subject(
                    kind="Group",
                    name=f"xdew-project-{project_id}-admins",
                    api_group="rbac.authorization.k8s.io"
                )
            ],
            role_ref=kubernetes.client.V1RoleRef(
                kind="Role",
                name=role_name,
                api_group="rbac.authorization.k8s.io"
            )
        )
        self.rbac_v1.create_namespaced_role_binding(k8s_namespace_name, role_binding)
    
    def _create_readonly_role(self, k8s_namespace_name: str, project_id: str,
                             owner_ref: Dict[str, Any]) -> None:
        """Create readonly role and binding."""
        role_name = f"xdew-{project_id}-readonly"
        
        # Create readonly role
        role = kubernetes.client.V1Role(
            metadata=kubernetes.client.V1ObjectMeta(
                name=role_name,
                namespace=k8s_namespace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            rules=self._get_readonly_policy_rules()
        )
        self.rbac_v1.create_namespaced_role(k8s_namespace_name, role)
        
        # Create readonly role binding
        role_binding = kubernetes.client.V1RoleBinding(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{role_name}-binding",
                namespace=k8s_namespace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            subjects=[
                kubernetes.client.V1Subject(
                    kind="Group",
                    name=f"xdew-project-{project_id}-readonly",
                    api_group="rbac.authorization.k8s.io"
                )
            ],
            role_ref=kubernetes.client.V1RoleRef(
                kind="Role",
                name=role_name,
                api_group="rbac.authorization.k8s.io"
            )
        )
        self.rbac_v1.create_namespaced_role_binding(k8s_namespace_name, role_binding)
    
    @staticmethod
    def _get_admin_policy_rules() -> List[kubernetes.client.V1PolicyRule]:
        """Get policy rules for admin role."""
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
    
    @staticmethod
    def generate_kubernetes_namespace_name(project_id: str, namespace_id: str) -> str:
        """Generate a valid Kubernetes namespace name."""
        hash_input = f"{project_id}-{namespace_id}"
        hash_suffix = hashlib.md5(hash_input.encode()).hexdigest()[:8]
        name = f"xdew-{project_id}-{namespace_id}-{hash_suffix}"
        return name[:63].lower()
    
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
    
    def create_namespace_resources(self, namespace_info: NamespaceInfo,
                                 project_info: ProjectInfo, owner_ref: Dict[str, Any]) -> bool:
        """Create all resources for a namespace."""
        k8s_namespace_name = self.generate_kubernetes_namespace_name(
            namespace_info.project_id, namespace_info.namespace_id
        )
        
        try:
            # Create namespace
            if not self.namespace_manager.create(
                k8s_namespace_name, namespace_info.project_id,
                namespace_info.display_name, owner_ref, project_info
            ):
                return False
            
            # Create resource quota
            quota = namespace_info.resource_quota or self.get_default_resource_quota()
            if not self.quota_manager.create(k8s_namespace_name, quota, owner_ref):
                return False
            
            # Create RBAC resources
            if not self.rbac_manager.create(
                k8s_namespace_name, namespace_info.project_id, owner_ref
            ):
                return False
            
            namespace_info.kubernetes_namespace = k8s_namespace_name
            return True
            
        except Exception as e:
            logger.error(f"Failed to create namespace resources: {e}")
            return False
    
    def delete_namespace_resources(self, kubernetes_namespace: str) -> bool:
        """Delete all resources for a namespace."""
        return self.namespace_manager.delete(kubernetes_namespace)
    
    def update_project_status(self, name: str, status_update: Dict[str, Any]) -> None:
        """Update project status."""
        try:
            project = self.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="projects",
                name=name
            )
            
            if "status" not in project:
                project["status"] = {}
            
            project["status"].update(status_update)
            project["status"]["lastUpdated"] = datetime.now(timezone.utc).isoformat()
            
            self.custom_api.patch_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="projects",
                name=name,
                body=project
            )
            logger.info(f"Updated project status: {name}")
        except Exception as e:
            logger.error(f"Failed to update project status: {e}")
    
    def update_namespace_status(self, name: str, status_update: Dict[str, Any]) -> None:
        """Update namespace status."""
        try:
            namespace = self.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="namespaces",
                name=name
            )
            
            if "status" not in namespace:
                namespace["status"] = {}
            
            namespace["status"].update(status_update)
            namespace["status"]["lastUpdated"] = datetime.now(timezone.utc).isoformat()
            
            self.custom_api.patch_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="namespaces",
                name=name,
                body=namespace
            )
            logger.info(f"Updated namespace status: {name}")
        except Exception as e:
            logger.error(f"Failed to update namespace status: {e}")
    
    def update_project_namespace_count(self, project_id: str) -> None:
        """Update the namespace count for a project."""
        try:
            project_info = self.get_project_by_id(project_id)
            if not project_info:
                return
            
            namespaces = self.custom_api.list_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="workspaces"
            )
            
            count = sum(
                1 for ns in namespaces.get("items", [])
                if ns.get("spec", {}).get("projectId") == project_id
            )
            
            self.update_project_status(project_id, {"namespacesCount": count})
            
        except Exception as e:
            logger.error(f"Failed to update project namespace count: {e}")
    
    def cleanup_project_namespaces(self, project_id: str) -> None:
        """Clean up all workspaces associated with a project."""
        try:
            namespaces = self.custom_api.list_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="workspaces"
            )
            
            for ns in namespaces.get("items", []):
                if ns.get("spec", {}).get("projectId") == project_id:
                    ns_name = ns["metadata"]["name"]
                    logger.info(f"Deleting associated workspace: {ns_name}")
                    
                    try:
                        self.custom_api.delete_cluster_custom_object(
                            group="xdew.ch",
                            version="v1",
                            plural="workspaces",
                            name=ns_name
                        )
                    except Exception as e:
                        logger.error(f"Failed to delete workspace {ns_name}: {e}")
                        
        except Exception as e:
            logger.error(f"Failed to cleanup project workspaces: {e}")


# Global operator instance
operator = XDEWOperator()


@kopf.on.create('xdew.ch', 'v1', 'projects')
def create_project(spec, name, patch, **kwargs):
    """Handle project creation events."""
    logger.info(f"Creating project: {name}")
    
    try:
        # Validate required fields
        project_name = spec.get('name')
        user_id = spec.get('userId')
        
        if not project_name or not user_id:
            patch.status['error'] = "Missing required fields: name or userId"
            return
        
        # Update status
        patch.status['createdAt'] = datetime.now(timezone.utc).isoformat()
        patch.status['namespacesCount'] = 0
        
        logger.info(f"Project {name} created successfully")
        
    except Exception as e:
        logger.error(f"Failed to create project {name}: {e}")
        patch.status['error'] = str(e)


@kopf.on.create('xdew.ch', 'v1', 'workspaces')
def create_workspace(spec, name, patch, **kwargs):
    """Handle workspace creation events."""
    logger.info(f"Creating workspace: {name}")
    
    try:
        # Extract namespace information
        display_name = spec.get('name', '')
        project_id = spec.get('projectId')
        description = spec.get('description', '')
        resource_quota_spec = spec.get('resourceQuota', {})
        
        # Validate project exists
        project_info = operator.get_project_by_id(project_id)
        if not project_info:
            patch.status['state'] = NamespaceState.REJECTED.value
            patch.status['message'] = f"Project {project_id} not found"
            patch.status['createdAt'] = datetime.now(timezone.utc).isoformat()
            logger.warning(f"Project {project_id} not found for workspace {name}")
            return
        
        # Set default resource quota if not provided
        if not resource_quota_spec:
            resource_quota_spec = operator.get_default_resource_quota().to_dict()
            patch.spec['resourceQuota'] = resource_quota_spec
            logger.info(f"Applied default resource quota to workspace: {name}")
        
        # Update status
        patch.status['state'] = NamespaceState.REQUESTED.value
        patch.status['message'] = "Workspace creation requested"
        patch.status['createdAt'] = datetime.now(timezone.utc).isoformat()
        
        # Update project namespace count
        operator.update_project_namespace_count(project_id)
        
        logger.info(f"Workspace {name} created in requested state")
        
    except Exception as e:
        logger.error(f"Failed to create workspace {name}: {e}")
        patch.status['state'] = NamespaceState.REJECTED.value
        patch.status['message'] = f"Creation failed: {str(e)}"
        patch.status['createdAt'] = datetime.now(timezone.utc).isoformat()


@kopf.on.field('xdew.ch', 'v1', 'workspaces', field='status.state')
def handle_workspace_state_change(old, new, spec, name, uid, **kwargs):
    """Handle workspace state changes."""
    logger.info(f"Workspace {name} state changed from {old} to {new}")
    
    if new == NamespaceState.ACCEPTED.value:
        _handle_workspace_acceptance(spec, name, uid)
    elif new == NamespaceState.DELETED.value:
        _handle_workspace_deletion(name)


def _handle_workspace_acceptance(spec, name, uid):
    """Handle workspace acceptance logic."""
    try:
        # Extract namespace information
        display_name = spec.get('name', '')
        project_id = spec.get('projectId')
        resource_quota_spec = spec.get('resourceQuota', {})
        
        # Get project information
        project_info = operator.get_project_by_id(project_id)
        if not project_info:
            status_update = {
                "state": NamespaceState.REJECTED.value,
                "message": f"Project {project_id} not found"
            }
            operator.update_namespace_status(name, status_update)
            return
        
        # Create namespace info
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
        
        # Create namespace resources
        if operator.create_namespace_resources(namespace_info, project_info, owner_ref):
            status_update = {
                "state": NamespaceState.ACCEPTED.value,
                "message": "Workspace accepted and resources created",
                "kubernetesNamespace": namespace_info.kubernetes_namespace
            }
        else:
            status_update = {
                "state": NamespaceState.REJECTED.value,
                "message": "Failed to create workspace resources"
            }
        
        operator.update_namespace_status(name, status_update)
        
    except Exception as e:
        logger.error(f"Failed to handle workspace acceptance for {name}: {e}")
        status_update = {
            "state": NamespaceState.REJECTED.value,
            "message": f"Failed to create resources: {str(e)}"
        }
        operator.update_namespace_status(name, status_update)


def _handle_workspace_deletion(name):
    """Handle workspace deletion logic."""
    try:
        namespace = operator.custom_api.get_cluster_custom_object(
            group="xdew.ch",
            version="v1",
            plural="workspaces",
            name=name
        )
        
        k8s_namespace_name = namespace.get("status", {}).get("kubernetesNamespace")
        if k8s_namespace_name:
            operator.delete_namespace_resources(k8s_namespace_name)
            
    except Exception as e:
        logger.error(f"Failed to delete resources for workspace {name}: {e}")


@kopf.on.delete('xdew.ch', 'v1', 'workspaces')
def delete_workspace(spec, name, patch, **kwargs):
    """Handle workspace deletion events."""
    logger.info(f"Deleting workspace: {name}")
    
    try:
        project_id = spec.get('projectId')
        
        # Update status
        patch.status['state'] = NamespaceState.DELETED.value
        patch.status['message'] = "Workspace deletion in progress"
        
        # Update project namespace count
        if project_id:
            operator.update_project_namespace_count(project_id)
            
    except Exception as e:
        logger.error(f"Failed to handle workspace deletion for {name}: {e}")


@kopf.on.delete('xdew.ch', 'v1', 'projects')
def delete_project(name, **kwargs):
    """Handle project deletion events."""
    logger.info(f"Deleting project: {name}")
    
    try:
        # Clean up associated namespaces
        operator.cleanup_project_namespaces(name)
        
    except Exception as e:
        logger.error(f"Failed to handle project deletion for {name}: {e}")


def main():
    """Main entry point for the operator."""
    logger.info("Starting XDEW Kubernetes Operator")
    kopf.configure(verbose=True)
    kopf.run()


if __name__ == "__main__":
    main()
